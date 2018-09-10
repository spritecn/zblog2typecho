#coding:utf8
#zb:zblog,tc:typecho
#

import toml
from utils import  tomlParse,dbConntect,tablePrefixDict

_tomlLoad = tomlParse()
zbDbConfig = _tomlLoad['zblog']
tcDbConfig = _tomlLoad['typecho']
postRawId = _tomlLoad['post']['raw_post_id']

zbTableList = ['category','tag','post','comment']
tcTableList = ['metas','contents','comments','relationships']
zbTableDict = tablePrefixDict(zbTableList,zbDbConfig['table_prefix'])
tcTableDict = tablePrefixDict(tcTableList,tcDbConfig['table_prefix'])

zbCon = dbConntect(zbDbConfig)
tcCon = dbConntect(tcDbConfig)




#category
def cate_do():
    zbCur = zbCon.cursor()
    tcCur = tcCon.cursor()
    zbCateQueryStr = f"select cate_name,cate_ID,cate_Alias from {zbTableDict['category']}"
    zbCur.execute(zbCateQueryStr)
    zbCateQueryResult = zbCur.fetchall()
    for i in zbCateQueryResult:
        _str = f"select name from {tcTableDict['metas']} where name='{i[0]}' and type='category'"
        tcCur.execute(_str)
        if  tcCur.fetchone():continue
        insertStr = f"insert into {tcTableDict['metas']} (name,slug,type) values ('{i[0]}','{i[2]}','category')"
        #print(insertStr)
        tcCur.execute(insertStr)
    tcCon.commit()
    tcCur.execute(f"select name,mid from {tcTableDict['metas']} where type='category'")
    tcMetaQueryResult = tcCur.fetchall()
    
    cateIdMetaIdDict = {}
    for _x in zbCateQueryResult:
        for _y in tcMetaQueryResult:
            if _x[0] == _y[0]:
                cateIdMetaIdDict[str(_x[1])] = _y[1]
    zbCur.close()
    tcCur.close()
    return cateIdMetaIdDict
    

#tag
def tag_do():
    zbCur = zbCon.cursor()
    tcCur = tcCon.cursor()
    zbTagQueryStr = f"select tag_name,tag_ID from {zbTableDict['tag']}"
    zbCur.execute(zbTagQueryStr)
    zbTagQueryResult = zbCur.fetchall()
    for i in zbTagQueryResult:
        _str = f"select name from {tcTableDict['metas']} where name='{i[0]}' and type='tag'"
        tcCur.execute(_str)
        if  tcCur.fetchone():continue
        insertStr = f"insert into {tcTableDict['metas']} (name,type) values ('{i[0]}','tag')"
        #print(insertStr)
        tcCur.execute(insertStr)
    tcCon.commit()
    
    #fix slug
    tcCur.execute('update typecho_metas set slug=mid where slug is null')
    tcCon.commit()
    
    tcCur.execute(f"select name,mid from {tcTableDict['metas']} where type='tag'")
    tcMetaQueryResult = tcCur.fetchall()
    tagIdMetaIdDict = {}
    for _x in zbTagQueryResult:
        for _y in tcMetaQueryResult:
            if _x[0] == _y[0]:
                #print(_x,_y)
                tagIdMetaIdDict[str(_x[1])] = _y[1]
    zbCur.close()
    tcCur.close()
    return tagIdMetaIdDict
    
def getTcContentsMaxCid():
    tcCur = tcCon.cursor()
    tcCur.execute(f"select max(cid) from {tcTableDict['contents']}")
    _cid = tcCur.fetchone()[0]
    tcCur.close()
    return _cid

            

cateIdMetaIdDict = cate_do()
tagIdMetaIdDict = tag_do()

#post start
def post_do():
    postIdCidDict = {}
    zbCur = zbCon.cursor()
    tcCur = tcCon.cursor()
    tcContentsMaxCid = (getTcContentsMaxCid())
    if not tcContentsMaxCid:tcContentsMaxCid = 0
    zbPostQueryStr = f"select log_ID,log_CateID,log_Tag,log_type,log_Title,log_Content,log_PostTime,log_Alias from {zbTableDict['post']}"
    zbCur.execute(zbPostQueryStr)
    _cid = tcContentsMaxCid + 1
    _i = zbCur.fetchone()
    while _i:
        #type:post,page  status:publish
        print(_i[4])
        if postRawId:_cid = _i[0]
        mata_type = 'page' if _i[3] == 1 else 'post'
        insertContentsStr = f"insert into {tcTableDict['contents']}(cid,title,slug,created,text,authorId,type,status) values(%s,%s,%s,%s,%s,%s,%s,%s)"
        #insertStrValues =f"values('{_i[4]}','{_i[6]}','{_i[5]}','1','{mata_type}','publish')"
        #print(insertContentsStr)
        _slug = _i[7] if _i[7] else _cid
        _values = (_cid,_i[4],_slug,_i[6],_i[5],'1',mata_type,'publish')
        #print(_values)
        #insert content
        tcCur.execute(insertContentsStr,_values)
        
        #insert category  meta to  relationships
        if _i[1]:
            try:
                cateMetaId = cateIdMetaIdDict[str(_i[1])]
            except:
                print(f'Meta:{_i[1]} not exsits')
            insertRelationshipsStr = f"insert into {tcTableDict['relationships']}(cid,mid) values({_cid},{cateMetaId})"
            tcCur.execute(insertRelationshipsStr)
        
        #insert tag meta to relationships
        if _i[2]:
            for _t in _i[2].replace('{','').split('}')[:-1]:
                try:
                    tagMetaId = tagIdMetaIdDict[str(_t)]
                except:
                    print(f'tag:{_t} not exsits')
                    continue
                insertRelationshipsStr =  f"insert into {tcTableDict['relationships']}(cid,mid) values({_cid},{tagMetaId})"
                tcCur.execute(insertRelationshipsStr)
        postIdCidDict[str(_i[0])] = _cid
        _cid += 1
        _i = zbCur.fetchone()        
    tcCon.commit()
    tcCur.close()
    return postIdCidDict
postIdCidDIct = post_do()

#comment
zbCur = zbCon.cursor()
tcCur = tcCon.cursor()
commentIdDict = {}
#typecho comment table max coid
tcMaxCoid = 10
zbCommentQueryStr = "select comm_ID,comm_LogID,comm_ParentID,comm_Name,comm_Email,comm_HomePage,comm_Content,comm_PostTime,comm_IP " +(
                f"from {zbTableDict['comment']} order by comm_ID")
zbCur.execute(zbCommentQueryStr)
_c = zbCur.fetchone()
_coid = tcMaxCoid + 1
while _c:
    commInsertStr = f"insert into {tcTableDict['comments']}(coid,cid,created,author,mail,url,ip,text,parent,type,status) " +(
        "values(%s,%s,%s,%s,%s,%s,%s,%s,%s,'comment','approved')")
    #print(comentIdDict)
    try:
        cid = postIdCidDIct[str(_c[1])]
    except:
        print(f'{str(_c[1])} postId is not exsits')
        _c = zbCur.fetchone()
        _coid += 1
        continue
        
    parent = 0
    if _c[2]:
        try:
            parent = commentIdDict[_c[2]]
        except:
            print(f'{_c[2]}:parent not exsits')      
    _values = (_coid,cid,_c[7],_c[3],_c[4],_c[5],_c[8],_c[6],parent)
    tcCur.execute(commInsertStr,_values)
    commentIdDict[_c[0]] = _coid
    _c = zbCur.fetchone()
    _coid += 1
tcCon.commit()

zbCon.close()
tcCon.close()
print("-----finished-----------")


                    
                    
                    



    
    



    








