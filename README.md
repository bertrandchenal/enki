
# Enki

Enki implements the rsync algorithm to slice file content in blocks and
de-duplicate them. Those blocks are concatenated in a blob file and
their hashes are stored in a bolt database (saved in the `.nk`
sub-directory)


## Install

`go get bitbucket.org/bertrandchenal/enki/nk`


## Usage example

```
[bch@laptop tmp]$ nk snap
2018/04/10 08:07:33 Directory '.nk' created
2018/04/10 08:07:44 Add cdo-1.9.0.tar.gz
2018/04/10 08:07:44 Add go1.9.linux-amd64.tar.gz
2018/04/10 08:07:44 Add osquery-2.11.0_1.linux_x86_64.tar.gz
2018/04/10 08:07:44 Add postgresql-9.5.10-1-linux-x64-binaries.tar.gz

[bch@laptop tmp]$ du -hs *
9.1M    cdo-1.9.0.tar.gz
98M     go1.9.linux-amd64.tar.gz
17M     osquery-2.11.0_1.linux_x86_64.tar.gz
30M     postgresql-9.5.10-1-linux-x64-binaries.tar.gz

[bch@laptop tmp]$ du -hs .nk
206M    .nk

[bch@laptop tmp]$ cp go1.9.linux-amd64.tar.gz tmp1.tgz
[bch@laptop tmp]$ cp go1.9.linux-amd64.tar.gz tmp2.tgz
[bch@laptop tmp]$ cp go1.9.linux-amd64.tar.gz tmp3.tgz

[bch@laptop tmp]$ time nk snap
2018/04/10 08:08:35 Add tmp1.tgz
2018/04/10 08:08:35 Add tmp2.tgz
2018/04/10 08:08:35 Add tmp3.tgz

real    0m8.689s
user    0m9.089s
sys     0m0.503s

[bch@laptop tmp]$ du -hs *
9.1M    cdo-1.9.0.tar.gz
98M     go1.9.linux-amd64.tar.gz
17M     osquery-2.11.0_1.linux_x86_64.tar.gz
30M     postgresql-9.5.10-1-linux-x64-binaries.tar.gz
98M     tmp1.tgz
98M     tmp2.tgz
98M     tmp3.tgz

[bch@laptop tmp]$ du -hs .nk
207M    .nk

[bch@laptop tmp]$ nk log
2018-04-10T08:08:26
2018-04-10T08:07:33

[bch@laptop tmp]$ rm tmp*
[bch@laptop tmp]$ time nk re
2018/04/10 08:09:37 Restore tmp3.tgz
2018/04/10 08:09:39 Restore tmp1.tgz
2018/04/10 08:09:41 Restore tmp2.tgz

real    0m7.154s
user    0m7.042s
sys     0m0.387s

[bch@laptop tmp]$ nk re 2018-04-10T08:07:33 # Restore the oldest snapshot
2018/04/10 08:10:04 Delete tmp1.tgz
2018/04/10 08:10:04 Delete tmp2.tgz
2018/04/10 08:10:04 Delete tmp3.tgz

[bch@laptop tmp]$ du -hs *
9.1M    cdo-1.9.0.tar.gz
98M     go1.9.linux-amd64.tar.gz
17M     osquery-2.11.0_1.linux_x86_64.tar.gz
30M     postgresql-9.5.10-1-linux-x64-binaries.tar.gz

[bch@laptop tmp]$ du -hs .nk
207M    .nk

[bch@backtesting tmp]$ du -h .nk/*
206M    .nk/blocks.blob
948K    .nk/indexes.bolt
268K    .nk/sigs.blob
16K     .nk/weakmap.gob
```


## Files content

The `blocks.blob` contains all deduplicated blocks. The `sigs.blob`
contains all the signatures, a signature is a list of the blocks
hashes that compose the file.

The `indexes.bolt` is a bolt db that contains

  - a map of md5 hashes to their respective block offset in the block file
  - a list of directory state (each state is the list of all the files
    and their hashes)
  - a map of file hashes to their signatures offset.


The `weakmap.gob` file contains a list of weak checksums of all the
blocks. This act as a bloom filter wrt to the bolt db: If a weak
cheksum of a block is not in the weakmap, we know that a the
(stronger) md5 cheksim of the block wont be present in `indexes.bolt`
