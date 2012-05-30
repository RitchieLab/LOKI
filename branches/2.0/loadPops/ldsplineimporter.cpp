/* 
 * File:   ldsplineimporter.cpp
 * Author: torstees
 * 
 * Created on September 17, 2010, 1:50 PM
 */
#include "ldsplineimporter.h"
#include "ldspline/ldspline.h"
#include <fstream>

#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <sys/stat.h>
#include <sstream>

#include <utility>

using std::pair;
using std::make_pair;
using std::stringstream;

string __chr_init[] = {"1","2","3","4","5","6","7","8","9","10",
		"11","12","13","14","15","16","17","18","19","20","21","22","X",
		"Y","XY","MT"};

// Make sure the # below matches the # of elements in the array above!!
const vector<string> LdSplineImporter::_chrom_list(__chr_init, __chr_init + (sizeof(__chr_init) / sizeof(__chr_init[0])));

const string LdSplineImporter::_tmp_bnd_tbl("__region_bound_tmp");

LdSplineImporter::LdSplineImporter(const string& fn, const string& db_fn) :
		_self_open(true), _write_db(false) {
	LoadConfiguration(fn.c_str());

	dbFilename = db_fn;
	boost::filesystem::path dbPath = boost::filesystem::path(db_fn);
	bool fileFound = false;
	if (boost::filesystem::is_regular_file(dbPath)) {
		fileFound = true;
	} else {
#ifdef DATA_DIR
		if (dbPath.is_relative()) {
			dbPath = (boost::filesystem::path(std::string(DATA_DIR))/=(dbPath));
			if (boost::filesystem::is_regular_file(dbPath)) {
				fileFound=true;
				dbFilename=dbPath.string();
			}
		}
#endif
	}

	if (!fileFound){
		throw std::runtime_error("DB File not found");
	}

	// At this point, try to get write permissions, if needed

	// BEGIN Non-portable code!!!
	struct stat results;
	bool throw_err = false;

	// If we do not currently have write access
	if (stat(dbPath.c_str(), &results)) {
		throw_err = true;
	} else if (!(results.st_mode & S_IWUSR)) {
		//set the write access
		if (!chmod(dbPath.c_str(), results.st_mode | S_IWUSR)) {
			//Whoo-hoo, it's writeable!
			_write_db = true;
		} else {
			throw_err = true;
			//Uh-oh, can't set the write bit.  Time to throw an error!
		}
	} // Hidden else means that we found that write bit was already set; noop

	// END Non-portable code
	if (throw_err) {
		throw std::runtime_error("Cannot write to Database");
	}

	// Create the sqlite3 db object
	sqlite3_open(dbFilename.c_str(), &_db);

	LoadGenes();
}

LdSplineImporter::LdSplineImporter(const string& fn, sqlite3 *db_conn) :
		_db(db_conn), _self_open(false), _write_db(false) {
	LoadConfiguration(fn.c_str());
	// all of the DB stuff is done for us!!
	LoadGenes();
}

LdSplineImporter::~LdSplineImporter() {
	if (_self_open) {
		sqlite3_close(_db);
	}

	// If we set the write bit, it's now time to unset it
	if(_write_db){
		struct stat results;
		if(!stat(dbFilename.c_str(), &results)){
			chmod(dbFilename.c_str(), results.st_mode & (~S_IWUSR));
		}
	}
}

void LdSplineImporter::loadPops() {

	// Create a temporary table to insert into
	string tmp_cmd = "CREATE TEMPORARY TABLE " + _tmp_bnd_tbl +
			"(region_id INTEGER NOT NULL, population_id INTEGER NOT NULL, "
			"chr TINYINT NOT NULL, posMin BIGINT NOT NULL, "
			"posMax BIGINT NOT NULL, source_id TINYINT NOT NULL)";

	sqlite3_exec(_db, tmp_cmd.c_str(), NULL, NULL, NULL);

	// First, collect all of the indexes
	map<string, string> index_map;
	string idx_cmd = "SELECT name, sql FROM sqlite_master "
			"WHERE type='index' AND tbl_name='region_bound' AND sql NOT NULL";

	sqlite3_exec(_db, idx_cmd.c_str(), &parseRegionIndex, &index_map, NULL);

	// Now, drop those indexes!
	string drop_cmd = "DROP INDEX ";

	map<string, string>::const_iterator idx_itr = index_map.begin();
	while(idx_itr != index_map.end()){
		string drop_tbl = "'" + (*idx_itr).first + "'";
		string sql_str = drop_cmd + drop_tbl;
		sqlite3_exec(_db, (drop_cmd + drop_tbl).c_str(), NULL, NULL, NULL);
		++idx_itr;
	}

	vector<PopulationSpline>::const_iterator spItr = splines.begin();
	vector<PopulationSpline>::const_iterator spEnd = splines.end();

	while (spItr != spEnd) {
		map<string, int> popIDs;
		InitPopulationIDs(popIDs, *spItr);

		LdSpline ldspline;
		ldspline.OpenBinary(spItr->filename.c_str());

		map<string, LocusLookup>& chromosomes =
				ldspline.GetChromosomes();
		map<string, LocusLookup>::iterator chr = chromosomes.begin();
		map<string, LocusLookup>::iterator end = chromosomes.end();

		while (chr != end) {
			ProcessLD(chr->second, *spItr, popIDs);
			chr->second.Release();
			++chr;
		}
		++spItr;
	}

	// Move everything from the temporary table into the "real" table
	insert_sql = "INSERT OR IGNORE INTO region_bound "
			"SELECT * from " + _tmp_bnd_tbl;
	sqlite3_exec(_db, insert_sql.c_str(), NULL, NULL, NULL);

	// Recretate the indexes
	idx_itr = index_map.begin();
	while(idx_itr != index_map.end()){
		sqlite3_exec(_db, (*idx_itr).second.c_str(), NULL, NULL, NULL);
		++idx_itr;
	}

	//Update the zone table
	UpdateZones();

}

void LdSplineImporter::UpdateZones(){

}

/**
 * @brief Parse configuration
 * @param filename
 *
 * Example:
 * rs 0.9 0.8 0.6
 * dp 0.9 0.8 0.6
 * CEU /path/to/ceu.ldspline Descriptive note about CEU population
 * JPT /path/to/jpg.ldspline Descriptive note about the population
 * ...
 */
void LdSplineImporter::LoadConfiguration(const char *filename) {
	std::ifstream file(filename);
	while (file.good() && !file.eof()) {
		char line[4096];
		file.getline(line, 4096);

		std::stringstream ss(line);

		std::istream_iterator<std::string> itr(ss);

		std::vector<std::string> tokens(itr,
				std::istream_iterator<std::string>());

		if (tokens.size() > 0) {
			if (tokens[0] == "rs" || tokens[0] == "RS") {
				std::vector<std::string>::iterator values = tokens.begin();
				std::vector<std::string>::iterator tokenEnd = tokens.end();
				while (++values != tokenEnd) {
					cutoffs.push_back(make_pair(R_SQUARED, atof(values->c_str())));
				}
			} else if (tokens[0] == "dp" || tokens[0] == "DP") {
				std::vector<std::string>::iterator values = tokens.begin();
				std::vector<std::string>::iterator tokenEnd = tokens.end();
				while (++values != tokenEnd) {
					cutoffs.push_back(make_pair(D_PRIME, atof(values->c_str())));
				}
			} else {
				if (tokens[0][0] != '#') {
					std::stringstream ss(line);
					std::string pop, popFilename, word;
					ss >> pop >> popFilename;

					std::stringstream desc;
					int k=-1;
					while (!ss.eof()) {
						ss >> word;
						if(++k){
							desc << " ";
						}
						desc << word;
					}

					splines.push_back(
							PopulationSpline(pop, desc.str(), popFilename));
				}
			}
		}
	}
}

void LdSplineImporter::ProcessLD(LocusLookup& chr,
		const PopulationSpline& sp, const map<std::string, int>& popIDs) {

	short chrom = getChrom(chr.Chromosome());

	vector<RegionBoundary>::const_iterator regItr = _region_map[chrom].begin();
	vector<RegionBoundary>::const_iterator regEnd = _region_map[chrom].end();

	cerr << chr.Chromosome() << "(";
	cerr.flush();
	map<std::string, int>::const_iterator pi = popIDs.begin();
	map<std::string, int>::const_iterator pe = popIDs.end();

	while (pi != pe) {
		cerr << pi->first << " ";
		cerr.flush();
		pi++;
	}
	string pos_cmd = "INSERT INTO " + _tmp_bnd_tbl +
			"(region_id, population_id, chr, posMin, posMax, source_id) VALUES "
			"(:rid, :pid, :ochr, :new_min, :new_max, :sid)";

	sqlite3_stmt* pos_stmt;
	sqlite3_prepare_v2(_db, pos_cmd.c_str(), -1, &pos_stmt, NULL);;

	int n_min_idx = sqlite3_bind_parameter_index(pos_stmt, ":new_min");
	int n_max_idx = sqlite3_bind_parameter_index(pos_stmt, ":new_max");
	int r_id_idx = sqlite3_bind_parameter_index(pos_stmt, ":rid");
	int p_id_idx = sqlite3_bind_parameter_index(pos_stmt, ":pid");
	int chr_id_idx = sqlite3_bind_parameter_index(pos_stmt, ":ochr");
	int src_id_idx = sqlite3_bind_parameter_index(pos_stmt, ":sid");

	sqlite3_bind_int(pos_stmt, chr_id_idx, chrom);

	vector<pair<CutoffType, float> >::const_iterator v_itr = cutoffs.begin();
	while(v_itr != cutoffs.end()){
		regItr = _region_map[chrom].begin();
		map<string, int>::const_iterator pop_itr = popIDs.find(
				sp.GetPopulationName((*v_itr).first, (*v_itr).second));
		if (pop_itr != popIDs.end()) {
			sqlite3_bind_int(pos_stmt, p_id_idx, (*pop_itr).second);

			while (regItr != regEnd) {

				sqlite3_bind_int(pos_stmt, r_id_idx, (*regItr).geneID);
				sqlite3_bind_int(pos_stmt, src_id_idx, (*regItr).source_id);

				pair<int, int> bounds;
				bool valid_region = false;
				if((*v_itr).first == D_PRIME){
					bounds = chr.GetRangeBoundariesDP((*regItr).lower, (*regItr).upper,	(*v_itr).second);
					valid_region=true;
				}else if ((*v_itr).first == R_SQUARED){
					bounds = chr.GetRangeBoundariesRS((*regItr).lower, (*regItr).upper, (*v_itr).second);
					valid_region=true;
				}

				if (valid_region){
					sqlite3_bind_int(pos_stmt, n_min_idx, bounds.first);
					sqlite3_bind_int(pos_stmt, n_max_idx, bounds.second);

					// Do nothing for the query; it modifies the db
					while(sqlite3_step(pos_stmt) == SQLITE_ROW) {}
				}
				sqlite3_reset(pos_stmt);
				++regItr;
			}
		}
		++v_itr;
	}

	sqlite3_finalize(pos_stmt);

	cerr << ")\n";
}

void LdSplineImporter::InitPopulationIDs(map<string, int>& popIDs,
		const PopulationSpline& sp) {

	vector<pair<CutoffType, float> >::const_iterator sItr = cutoffs.begin();

	while (sItr != cutoffs.end()) {
		string popName = sp.GetPopulationName((*sItr).first, (*sItr).second);

		string pop_query = "SELECT population_id FROM population where population='"+popName+"';";
		int popID = -1;

		sqlite3_exec(_db, pop_query.c_str(), parsePopID, &popID, NULL);

		if (popID == -1) {

			stringstream pop_ins_ss;
			string type = ((*sItr).first == R_SQUARED ? "RS" : ((*sItr).first == D_PRIME ? "DP" : "UNK"));
			pop_ins_ss << "INSERT INTO population (population, ldcomment, description) "
					<< "VALUES ('" << popName << "','" << type << " " << (*sItr).second << "','"
					<< sp.desc << " with " << type << " cutoff " << (*sItr).second << "');";

			sqlite3_exec(_db, pop_ins_ss.str().c_str(), NULL, NULL, NULL);
			sqlite3_exec(_db, pop_query.c_str(), parsePopID, &popID, NULL);

		} else {
			stringstream del_ss;
			del_ss << "DELETE FROM region_bound WHERE population_id=" << popID <<"; ";
			
			sqlite3_exec(_db, del_ss.str().c_str(), NULL, NULL, NULL);
		}

		//stringstream bds_ins_ss;

		//bds_ins_ss << "INSERT INTO region_bound SELECT region_id, " << popID
		//		<< ", chr, posMin, posMax, source_id FROM region_bound WHERE population_id=1";
		//sqlite3_exec(_db, bds_ins_ss.str().c_str(), NULL, NULL, NULL);
	
		popIDs[popName] = popID;
		sItr++;
	}
}

void LdSplineImporter::LoadGenes() {

	stringstream query_ss;
	query_ss << "SELECT region_id, chr, posMin, posMax, region_bound.source_id "
			<< "FROM region_bound "
			<< "INNER JOIN region USING (region_id) "
			<< "INNER JOIN type ON region.type_id=type.type_id "
			<< "WHERE population_id=1 AND type='gene' "
			<< "ORDER BY chr, posMin;";

	sqlite3_exec(_db, query_ss.str().c_str(), parseGenes, &_region_map, NULL);

	cerr << "All Region Loaded \n";
}

int LdSplineImporter::parseGenes(void* obj, int n_cols, char** col_vals, char** col_names){
	if(n_cols != 5){
		return 2;
	}
	map<short, vector<RegionBoundary> >* result =
			(map<short, vector<RegionBoundary> >*) obj;
	(*result)[atoi(col_vals[1])].push_back(
			RegionBoundary(atoi(col_vals[0]), atoi(col_vals[2]), atoi(col_vals[3]), atoi(col_vals[4])));
	return 0;

}

int LdSplineImporter::parsePopID(void* pop_id, int n_cols, char** col_vals, char** col_names){
	if(n_cols !=  1){
		return 2;
	}

	int* result = (int*) pop_id;
	(*result) = atoi(col_vals[0]);
	return 0;
}

int LdSplineImporter::parseRegionIndex(void* obj, int n_cols, char** col_vals, char** col_names){
	if (n_cols != 2){
		return 2;
	}

	map<string, string>* idx_map_p = static_cast<map<string, string>*>(obj);
	(*idx_map_p)[col_vals[0]] = col_vals[1];
	return 0;
}

short LdSplineImporter::getChrom(const string& chrom_str){
	string eval_str = boost::to_upper_copy(chrom_str);

	// remove the 'CHR' at the beginning, should it exist
	if (eval_str.substr(0,3) == "CHR"){
		eval_str.erase(0,3);
	}

	// Catch a couple of special cases here...
	if (eval_str == "X|Y"){
		eval_str = "XY";
	}else if (eval_str == "M"){
		eval_str = "MT";
	}

	//Now, try to find in our vector, and give the position
	vector<string>::const_iterator chr_pos = find(_chrom_list.begin(),
			_chrom_list.end(), eval_str);

	// If this is true, we did not find an exact match, return -1
	if (chr_pos == _chrom_list.end()){
		return -1;
	}else{
		return chr_pos - _chrom_list.begin() + 1;
	}

}
