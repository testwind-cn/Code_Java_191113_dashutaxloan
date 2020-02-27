CREATE TABLE IF NOT EXISTS ${hivevar:DATABASE_SRC}.cjlog (
                                    `cid` string,
                                    `allnum` int,
                                    `begin_time` string,
                                    `cjlx` string,
                                    `finish_time` string,
                                    `mflag` int,
                                    `pnum` string,
                                    `productname` string,
                                    `taxname` string,
                                    `taxno` string,
                                    `oldtaxno` string
)