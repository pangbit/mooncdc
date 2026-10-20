# Synthetic audit parser fixtures

`copy-runtime-parser.json` contains fabricated source and executable hashes for
negative/structural tests in `cli-active-promotion-test.mbtx`. Its input names
exercise a legacy manifest format; they do not enumerate this release's product.
The `/tmp/mooncdc-parser-fixture` locations are fictional and need not exist.
`copy-storage.json` contains synthetic Docker storage-layout observations.
Neither file is a build receipt, a real machine dump, or authorization to launch
the historical prebuilt runtime profile. Run the portable full audit without
runtime-only flags, after provisioning isolated test services.
