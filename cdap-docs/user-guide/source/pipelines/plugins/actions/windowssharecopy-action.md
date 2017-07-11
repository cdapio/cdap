# Windows Share Copy


Description
-----------
Copies a file or files on a Microsoft Windows share to an HDFS directory.


Use Case
--------
This action can be used when a file or files need to be moved from a Windows share to an HDFS directory.


Properties
----------
**netBiosDomainName:** Specifies the NetBios domain name. Default is null. For example: `MyDomain`. (Macro-enabled)

**netBiosHostname:** Specifies the NetBios hostname to import files from. For example: `10.150.10.10`. (Macro-enabled)

**netBiosUsername:** Specifies the NetBios username to use when importing files from the Windows share. (Macro-enabled)

**netBiosPassword:** Specifies the NetBios password to use when importing files from the Windows share. (Macro-enabled)

**netBiosSharename:** Specifies the NetBios share name. For example: `share`. (Macro-enabled)

**numThreads:** Specifies the number of parallel tasks to use when executing the copy operation; defaults to 1. (Macro-enabled)

**sourceDirectory:** Specifies the NetBios directory or file. For example: `source` or `source/file.log`. (Macro-enabled)

**destinationDirectory:** The valid full HDFS destination path in the same cluster where
the file or files are to be moved. If a directory is specified as a destination with a
file as the source, the source file will be put into that directory. If the source is a
directory, it is assumed that destination is also a directory. This plugin does not check
and will not catch any inconsistency. (Macro-enabled)

**bufferSize:** The size of the buffer to be used for copying the files; minimum (and
default) buffer size is 4096; the value should be a multiple of the minimum size.
(Macro-enabled)

**overwrite:** Boolean that specifies if any matching files already present in the
destination should be overwritten or not; default is true.

Example
-------
This example copies all the files from `10.150.10.10:/source/share` to `example.com/dest/path`:

    {
        "name": "WindowsShareCopy",
        "plugin": {
            "name": "HDFSMove",
            "type": "action",
            "artifact": {
                "name": "core-plugins",
                "version": "1.4.1-SNAPSHOT",
                "scope": "SYSTEM"
            },
            "properties": {
                "netBiosDomainName": "domain",
                "netBiosHostname": "10.150.10.10",
                "netBiosUsername": "username",
                "netBiosPassword": "password",
                "netBiosSharename": "share",
                "sourceDirectory": "source",
                "destinationDirectory": "hdfs://example.com/dest/path",
                "bufferSize": "4096",
                "numThreads": "5"
            }
        }
    }

---
- CDAP Pipelines Plugin Type: action
- CDAP Pipelines Version: 1.7.0
