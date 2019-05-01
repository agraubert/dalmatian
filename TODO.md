# Lapdog > Dalmatian Integration

This branch is dedicated towards bringing all features which are not specific to
running lapdog jobs into dalmatian

## Workspace Model
- [x] Automated Entity uploads
  * Get rid of the separation between `prepare_*_df` and `upload_*`
- [x] Automated attribute uploads
  * Include lapdog's kwarg syntax

## Methods
- [ ] Add _latest_ handling
- [ ] Add handling for method metadata inference from config

## Operator
- [x] Replace assertions with APIExceptions
- [x] Integrate internal cache with lapdog parity
  * Actually remove the array translator from `upload_*` methods
  * Drop wdl from cache. No reason to have this without running jobs offline
- [ ] Integrate expression evaluator
- [ ] Add additional operator cache points
- [x] Add firecloud timeout shim
- [ ] Add background synchronizer

## CLI
- [ ] Add lapdog CLI for uploads
  * Accept arbitrary entity types as an argument
- [ ] Add lapdog CLI for methods
- [ ] Add lapdog CLI for workspace creation
- [ ] Add lapdog CLI for job submission

## Other
- [x] Migrate GetBlob API
- [ ] Integrate ACL API
- [x] Properties!

# Writeup

In this section, below the divider, I will keep track of the PR writeup

---

# Lapdog feature transfer

This PR aims to move many features from broadinstitute/lapdog into broadinstitute/dalmatian

Any features which were not specific to running Lapdog jobs have been migrated.

## New Features
* [Blob API](https://googleapis.github.io/google-cloud-python/latest/storage/index.html) now available via `dalmatian.getblob`
* `dalmatian.WorkspaceManager.upload_entities` now automatically checks for and uploads valid filepaths present in the dataframe
* `dalmatian.WorkspaceManager.update_attributes` now automatically checks for and uploads valid filepaths present in the attributes

## Other Changes
* `dalmatian.WorkspaceManager.update_attributes` can now take attributes as keyword arguments in addition to providing a premade dictionary
* Replaced `AssertionErrors` with `APIExceptions`, which are more descriptive and easier to handle