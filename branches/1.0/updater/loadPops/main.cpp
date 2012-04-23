/*
 * main.cpp
 *
 *  Created on: Apr 23, 2012
 *      Author: jrw32
 */

#include "ldsplineimporter.h"
#include <string>
#include <iostream>

using std::string;
using std::cout;

void printHelp(){

}

int main(int argc, char** argv){
	string db_fn = "knowledge.bio";
	string config_fn = "ldspline.cfg";

	// Parse the command line here
	int curr = 0;
	while (++curr < argc){
		string arg = argv[i];
		if((arg == "--DB" || arg == "-d") && curr < argc){
			db_fn = argv[++curr];
		}
		if((arg == "--config" || arg == "-c" ) && curr < argc){
			config_fn = argv[++curr];
		}
		if(arg == "--help" || arg == "-h"){
			printHelp();
			exit(1);
		}
	}


	try{
		LdSplineImporter lds(config_fn, db_fn);
		lds.loadPops();
	}catch(std::runtime_error& e){
		std::cerr<<"Caught error " << e.what() << "\n";
		return 1;
	}
	return 0;
	//
}



