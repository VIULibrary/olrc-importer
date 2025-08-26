# OLRC Importer #

Import AIPS and WARCs to Openstack (OLRC)

## USAGE ##
`arch-importer.py`  - Imports AIPs that have been exported from a previous Archivematica instance via [Arch-Exporter](https://github.com/VIULibrary/arch-exporter)

`warc-importer.py`- Imports WARCS that have been exported from Archive-IT 


1. Download and [source](https://docs.openstack.org/newton/user-guide/common/cli-set-environment-variables-using-openstack-rc.html)  your Openstack RC file credentials 
2. Install the [OpenStack and Swift clients](https://learn.scholarsportal.info/all-guides/cloud/tools/#Swift-Command-Line)
3. Note: the script calls `swiftclient.shell` to avoid confusion with the Apple `swift` command
3. Set your config parameters in the script: upload dir, segment size, container name
4. Run it: `python arch-importer.py` or `python warc-importer.py`
5. If using the `arch-importer.py` run `python filter.py` to remove sucessfully uploaded AIPs from your local upload directory. This is useful if you need to stop and start the script. You won't reload the AIPs in the upload directory

