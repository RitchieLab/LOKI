# LOKI Legacy Branch (2.4.x)

## Purpose

This branch (`legacy-2.4.x`) is isolated and exclusively dedicated to critical bug fixes and maintenance for the **LOKI version 2.4.x**. The version 2.4.x was originally developed in Python 2 and later adapted to Python 3 (version 2.4.3).

As development continues on the upcoming **LOKI version 3.x** in the `development` branch, the `legacy-2.4.x` branch will remain frozen for new features. **No pull requests from this branch to `master`, `development`, or any other branch are allowed**. All modifications must remain in this branch, and any changes relevant to the `development` branch will need to be manually reviewed and replicated.

This branch is maintained only for **critical bug fixes** that cannot wait for the release of LOKI version 3.x.

## Guidelines for Changes

- **No New Features**: This branch will only address critical bug fixes and performance issues essential for maintaining the stability of the 2.4.x series.
- **Manual Replication**: If a bug fix applied here is still relevant for LOKI version 3.x, the fix must be **manually replicated** in the `development` branch by the development team. No automated pull requests or merges are allowed between `legacy-2.4.x` and other branches.
- **Review Process**: All changes must be carefully reviewed and approved by at least one other team member before being merged into `legacy-2.4.x` to avoid introducing breaking changes.
- **Python 3 Compatibility**: The 2.4.x branch was ported to Python 3, so any changes made here must ensure continued compatibility with Python 3.

## Branch Workflow

1. **Isolated Changes**: Any work done in the `legacy-2.4.x` branch must remain isolated and **will not be merged into `master` or `development`**.
2. **Critical Bug Fixes Only**: The purpose of this branch is to address **severe bugs** that directly impact the performance or functionality of version 2.4.x. Non-critical issues should be addressed in future versions.
3. **Manual Replication for Development**: If any fix is relevant to the current `development` work, it must be **manually evaluated and replicated** in the `development` branch.

## Branch Policy

- **No Pull Requests to Other Branches**: This branch is isolated, and no changes from `legacy-2.4.x` will be pulled or merged into `master` or `development`. Any necessary changes must be manually applied to the `development` branch after careful consideration.
- **No New Features or Major Refactoring**: Any feature development or refactoring should happen exclusively in the `development` branch. This branch is for bug fixes only.

## Versioning

This branch will maintain the **2.4.x** version numbering for any bug fix releases (e.g., 2.4.4, 2.4.5). All new features, improvements, or refactoring should be implemented in the `development` branch, which is working towards version 3.x.

---

**Important:** This branch is dedicated to supporting systems still using LOKI version 2.4.x. Moving forward, we encourage all users to transition to the latest version in the `development` branch (3.x) once it's released.

For questions or issues related to this branch, please contact the maintainers.
