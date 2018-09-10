#coding:utf8

import sqlite3
import pymysql
import toml

def tomlParse():
    with open('config.toml','r',encoding='utf8') as f:
        return toml.loads(f.read())
        
def dbConntect(dbConfig):
    if dbConfig.get('db_type') == 'sqlite':
        return sqlite3.connect(dbConfig['sqlite_path'])
    else:
        _con = pymysql.connect(dbConfig['mysql_host'],
                dbConfig['mysql_user'],
                dbConfig['mysql_pass'],
                dbConfig['mysql_database'],
                dbConfig['mysql_port'] if dbConfig['mysql_port'] else 3306,
                charset='utf8')
        _con.autocommit(0)
        return _con

def tablePrefixDict(nameList,prefix):
    return dict(zip(nameList,[prefix+x for x in nameList]))

if __name__ == '__main__':
    #test                
    config = tomlParse()
    zbCon = dbConntect(config['zblog'])
    tcCon = dbConntect(config['typecho'])
                
