#!/usr/bin/python
# coding:utf-8
#
import os
import config
import JikuMap
import sys
import postgresql.driver as pg_driver
import re

# ------------------------------------------------
# グローバル
# ------------------------------------------------
gDbIspectConnection1 = None
gDbIspectConnection2 = None
gTabaleMngDbIspectConnection = None
gTableMap = {}
gClmMap = {}
# 処理用カラムマップ
gDoClmmap = {}
gObjALLDbIspectConnection = []
gOIDZIDMap = {}
# ------------------------------------------------
# 共通設定値
# ------------------------------------------------

DB_HOST1 = config.DB_HOST1
DB_PORT1 = config.DB_PORT1
DB_USER1 = config.DB_USER1
DB_PASS1 = config.DB_PASS1
DB_NAME1 = config.DB_NAME1
DB_SCHEMA1 = config.DB_SCHEMA1

DB_HOST2 = config.DB_HOST2
DB_PORT2 = config.DB_PORT2
DB_USER2 = config.DB_USER2
DB_PASS2 = config.DB_PASS2
DB_NAME2 = config.DB_NAME2
DB_SCHEMA2 = config.DB_SCHEMA2


OBJ_HOST = config.OBJ_HOST
OBJ_PORT = config.OBJ_PORT
OBJ_USER = config.OBJ_USER
OBJ_PASS = config.OBJ_PASS
OBJ_NAME = config.OBJ_DB_NAME
OBJ_SCHEMA = config.OBJ_SCHEMA

TBLMNG_HOST = config.TBLMNG_DB_HOST
TBLMNG_PORT = config.TBLMNG_DB_PORT
TBLMNG_USER = config.TBLMNG_DB_USER
TBLMNG_PASS = config.TBLMNG_DB_PASS
TBLMNG_NAME = config.TBLMNG_DB_NAME
TBLMNG_SCHEMA = config.TBLMNG_DB_SCHEMA

TBLMNG_DB_HOST=config.TBLMNG_DB_HOST
TBLMNG_DB_PORT=config.TBLMNG_DB_PORT
TBLMNG_DB_USER=config.TBLMNG_DB_USER
TBLMNG_DB_PASS=config.TBLMNG_DB_PASS
TBLMNG_DB_NAME=config.TBLMNG_DB_NAME
TBLMNG_DB_SCHEMA=config.TBLMNG_DB_SCHEMA

# ------------------------------------------------
# ロギング設定
# ------------------------------------------------
import logging

loggerDsp = logging.getLogger("loggerDisp")
loggerOnly = logging.getLogger("loggerOnlyFile")
# ログファイル出力先
formatter = logging.Formatter("%(asctime)s|%(lineno)04d|%(levelname)-6s|%(threadName)-16s|%(message)s")

logfile = "DB比較ツール.log"
fh = logging.FileHandler(logfile,"w")
fh.setFormatter(formatter)
loggerDsp.addHandler(fh)
loggerOnly.addHandler(fh)

# コンソールへの出力
sh = logging.StreamHandler()
sh.setFormatter(formatter)
loggerDsp.addHandler(sh)

# ログレベル
#LOGLEVEL=logging.DEBUG
LOGLEVEL=logging.INFO
#LOGLEVEL=logging.WARNING
#LOGLEVEL=logging.ERROR
#LOGLEVEL=logging.CRITICAL

loggerDsp.handlers
loggerDsp.setLevel(LOGLEVEL)

loggerOnly.handlers
loggerOnly.setLevel(LOGLEVEL)



#########################################################
#指定したプロジェクトIDが開始時点に承認されているプロジェクトIDリストを取得する
#########################################################
def getProjectList(projectID):
	
	# データベーススキーマ
	sSchema = config.TAF_SCHEMA
	# データベースオブジェクトの配列
	oDbConnection = pg_driver.connect(
	user      = config.TAF_USER,
	password  = config.TAF_PASS,
	database  = config.TAF_DB_NAME,
	host      = config.TAF_HOST,
	port	  = config.TAF_PORT)

	not_exist_pid_list = []

	# 承認状態を取得する
	# SQL文作成　※スキーマと、メッシュ番号は置換して指定する
	sQuery = '''WITH target_start_time AS (
                SELECT start_time 
                FROM SCHEMA.tb_dbm_mng_project_approval_manage
                WHERE project_id = PID AND db_class = 1)
                SELECT project_id 
                FROM SCHEMA.tb_dbm_mng_project_approval_manage, target_start_time 
                WHERE db_class = 1
                AND (
                (tb_dbm_mng_project_approval_manage.approval_time > target_start_time.start_time)
                OR (tb_dbm_mng_project_approval_manage.approval_time IS NULL))
                AND tb_dbm_mng_project_approval_manage.project_id NOT IN ( PID )'''.replace('SCHEMA',sSchema).replace('PID', str(projectID))

	# クエリ実行
	aResultRecordList = oDbConnection.query(sQuery)
	for record in aResultRecordList:
		not_exist_pid_list.append(str(record['project_id']))
	if len(not_exist_pid_list) == 0:
		not_exist_pid_list.append(9999)

	if len(not_exist_pid_list) != 1:
	   not_exist_pid_str = ",".join(not_exist_pid_list)
	else:
		not_exist_pid_str = str(not_exist_pid_list[0])
	return not_exist_pid_str

# ------------------------------------------------
# 生成元管理地物事象のクエリ
# ------------------------------------------------
def GetFtGenerator(sSchema,not_exist_pid_str,oDbConnection,aMeshBoxset):

	OIDRIDMap = {}
	TableIDRIDMap = {}
	RIDOIDMap = {}
	OIDtoRIDCnt = {}

	for sMeshBox in aMeshBoxset:
		sQuery = '''SELECT show_oid(generator_oid) as oid, generator_rid, creation_rid, t_represent_rid, 
							t_contents_id, t_box_hbr, t_llx_hbr,t_lly_hbr, t_urx_hbr, t_ury_hbr
					  FROM SCHEMA.tb_cgh_mn_generator_ft_mng
					  WHERE sdiff_start_project not in (PID_LIST) AND
	                        (sdiff_end_project is null or sdiff_end_project in (PID_LIST)) AND
	                        t_box_hbr && BOX'''.replace('SCHEMA',sSchema).replace('BOX',sMeshBox).replace('PID_LIST',not_exist_pid_str)
		# クエリ実行
		aResultRecordList = oDbConnection.query(sQuery)
		for aResultRecord in aResultRecordList:
			Genoid = aResultRecord['oid']
			Createrid = aResultRecord['creation_rid']
			tableID = int(str(Createrid)[:6])

			# テーブルIDRIDマップ作成
			insertridset = set()
			if tableID in TableIDRIDMap:
				insertridset = TableIDRIDMap[tableID]
			insertridset.add(Createrid)
			TableIDRIDMap[tableID] = insertridset
			# RIDOIDマップ作成
			insertoidset = set()
			if Createrid in RIDOIDMap:
				insertoidset = RIDOIDMap[Createrid]
			insertoidset.add(Genoid)
			RIDOIDMap[Createrid] = insertoidset
			# OIDRIDマップ作成
			insertridset = set()
			if Genoid in OIDRIDMap:
				insertridset = OIDRIDMap[Genoid]
			insertridset.add(Createrid)
			OIDRIDMap[Genoid] = insertridset
			# OIDに対するRIDの件数
			Cnt = 0
			if Genoid in OIDtoRIDCnt:
				Cnt = OIDtoRIDCnt[Genoid]
			OIDtoRIDCnt[Genoid] = 1 + Cnt

			
	return OIDRIDMap,TableIDRIDMap,RIDOIDMap,OIDtoRIDCnt
# ------------------------------------------------
# 生成元管理空間のクエリ
# ------------------------------------------------
def GetSpGenerator(sSchema,not_exist_pid_str,oDbConnection,aMeshBoxset):

	OIDRIDMap = {}
	TableIDRIDMap = {}
	RIDOIDMap = {}
	OIDtoRIDCnt = {}

# 空間の座標は？show_oid(generator_oid) is not null
	for sMeshBox in aMeshBoxset:
		sQuery = '''SELECT show_oid(generator_oid) as oid, generator_rid, creation_rid, t_represent_rid, 
							t_contents_id, t_box_hbr, t_llx_hbr,t_lly_hbr, t_urx_hbr, t_ury_hbr
					  FROM SCHEMA.tb_cgh_mn_generator_sp_mng
					  WHERE sdiff_start_project not in (PID_LIST) AND show_oid(generator_oid) is not null AND
	                        (sdiff_end_project is null or sdiff_end_project in (PID_LIST)) AND t_contents_id in (301001010,301001020,301001030,301001040,301001050,301001060,301001090,301001100,301001110,301001120,301001130,301001140,301001150,301001160,301001170,301001180,301001190,301001200,301001210,301001230,301001240,301001250,301001260,301001270,301001290,301001300,301001310,301001320,301001330,301001340,301001350,301001360,301001370,301001380,301001390,301001400,301001410,301001430,301003010,301003020,301003030,301003040,301003050,301003060,301003070,301003080,301003090,301003100,301003110,301003120,301003130,301003140,301003150,301003160,301003170,301003180,301003190,301003200,301003210,301003220,301003230,301003240,301003250,301003260,301003270,301003280,301003290,301004010) AND
	                        t_box_hbr && BOX'''.replace('SCHEMA',sSchema).replace('BOX',sMeshBox).replace('PID_LIST',not_exist_pid_str)
		# クエリ実行
		aResultRecordList = oDbConnection.query(sQuery)
		for aResultRecord in aResultRecordList:
			Genoid = aResultRecord['oid']
			Createrid = aResultRecord['creation_rid']
			tableID = int(str(Createrid)[:6])

			# テーブルIDRIDマップ作成
			insertridset = set()
			if tableID in TableIDRIDMap:
				insertridset = TableIDRIDMap[tableID]
			insertridset.add(Createrid)
			TableIDRIDMap[tableID] = insertridset
			# RIDOIDマップ作成
			insertoidset = set()
			if Createrid in RIDOIDMap:
				insertoidset = RIDOIDMap[Createrid]
			insertoidset.add(Genoid)
			RIDOIDMap[Createrid] = insertoidset
			# OIDRIDマップ作成
			insertridset = set()
			if Genoid in OIDRIDMap:
				insertridset = OIDRIDMap[Genoid]
			insertridset.add(Createrid)

			OIDRIDMap[Genoid] = insertridset
			# OIDに対するRIDの件数
			Cnt = 0
			if Genoid in OIDtoRIDCnt:
				Cnt = OIDtoRIDCnt[Genoid]
			OIDtoRIDCnt[Genoid] = 1 + Cnt

			
	return OIDRIDMap,TableIDRIDMap,RIDOIDMap,OIDtoRIDCnt


# ------------------------------------------------
# 生成先テーブルデータの取得
# ------------------------------------------------
def GetMapTableDate(not_exist_pid_str,OIDRIDMap,TableIDRIDMap,RIDOIDMap,DbIspectConnection,sSchema,OIDtoRIDCnt):
	
	sqlClmlist = []
	OIDRIDdate = {}
	OIDset = set()
	global gOIDZIDMap = {}
	CompOIDDateset = set()
	# テーブル毎にクエリ
	for tableid,ridset in TableIDRIDMap.items():
		logicaltable = gTableMap[tableid][0]
		ridlist = list(ridset)
		RIDList = (',').join(map(str,ridlist))
		# カラム取得
		Clmlist = gDoClmmap[tableid]
		sqlClm = (',').join(Clmlist)
		sqlClmlist = sqlClm.replace('\"','').replace('\'','')
		sQuery = '''
					select rid,substring(cast(rid as text),1 ,6) as tid,ClmList
					from SCHEMA.TABLE
					WHERE sdiff_start_project not in (PID_LIST) AND
	                        (sdiff_end_project is null or sdiff_end_project in (PID_LIST)) AND rid in (RIDList)
	                        
	              '''.replace('SCHEMA',sSchema).replace('PID_LIST',not_exist_pid_str).replace('TABLE',logicaltable).replace('RIDList',RIDList).replace('ClmList',sqlClmlist)
		aResultRecordList = DbIspectConnection.query(sQuery)
		outputClmList = ['rid','tid'] + Clmlist
		for aResultRecord in aResultRecordList:
			genoidset = RIDOIDMap[aResultRecord['rid']]
			iOIDCnt = 0
			# RID１つに対してOID複数は存在する
			for genoid in genoidset:
				iOIDCnt = iOIDCnt + 1
				insertdatelist = []
				# OIDとRID+RIDデータのマップ作成
				if genoid in OIDRIDdate:
					insertdatelist = OIDRIDdate[genoid]
				if iOIDCnt == 1:
					headinsertdate = "生成先"+  ' , '
				else:
					headinsertdate = ";生成先"+ ' , '
				insertdate = ""
				for Clm in outputClmList:
					Clm = Clm.replace('show_zid(zenrin_id) as zid','zid')
					insertdate = insertdate + str(Clm) + ':' + str(aResultRecord[Clm]) + ' , '
				insertdate = headinsertdate + insertdate
				insertdateline = insertdate[:-3]
				insertdatelist.append(insertdateline)
				OIDRIDdate[genoid] = insertdatelist
				# OIDセット
				OIDset.add(genoid)
			# 一つのOIDに対して複数のRID考慮
			# OIDとRIDデータのset作成
			for genoid,OIDRIDdateList in OIDRIDdate.items():
				if genoid in OIDtoRIDCnt:
					OIDCnt = OIDtoRIDCnt[genoid]
				else:
					pass
					# エラー出力
					OIDCnt = -1
				for riddateline in OIDRIDdateList:
					insertline = re.sub('rid(.*)tid','tid',riddateline)
					insertline = 'OIDに対するRIDの件数:' + str(OIDCnt) + ' , ' + 'OID:'+ genoid + ' , ' +insertline
				CompOIDDateset.add(insertline)
			
	
	return OIDRIDdate,CompOIDDateset,OIDset

# ------------------------------------------------
# 地物事象のクエリ
# ------------------------------------------------
def GetFtQueryDate(not_exist_pid_list,aMeshBoxset):

	# 生成元管理の取得
	sSchema1 = DB_SCHEMA1
	OIDRIDMap1,TableIDRIDMap1,RIDOIDMap1,OIDtoRIDCnt1 = GetFtGenerator(sSchema1,not_exist_pid_list,gDbIspectConnection1,aMeshBoxset)

	sSchema2 = DB_SCHEMA2
	OIDRIDMap2,TableIDRIDMap2,RIDOIDMap2,OIDtoRIDCnt2 = GetFtGenerator(sSchema2,not_exist_pid_list,gDbIspectConnection2,aMeshBoxset)

	# 生成先テーブルデータの取得1
	OIDRIDResultRecord1,CompOIDDateMap1,OIDset1 = GetMapTableDate(not_exist_pid_list,OIDRIDMap1,TableIDRIDMap1,RIDOIDMap1,gDbIspectConnection1,sSchema1,OIDtoRIDCnt1)

	# 生成先テーブルデータの取得2
	OIDRIDResultRecord2,CompOIDDateMap2,OIDset2 = GetMapTableDate(not_exist_pid_list,OIDRIDMap2,TableIDRIDMap2,RIDOIDMap2,gDbIspectConnection2,sSchema2,OIDtoRIDCnt2)



	return OIDRIDResultRecord1,OIDRIDResultRecord2,CompOIDDateMap1,CompOIDDateMap2,OIDset1,OIDset2


# ------------------------------------------------
# 空間のクエリ
# ------------------------------------------------
def GetSpQueryDate(not_exist_pid_list,aMeshBoxset):

	# 生成元管理の取得
	sSchema1 = DB_SCHEMA1
	OIDRIDMap1,TableIDRIDMap1,RIDOIDMap1,OIDtoRIDCnt1 = GetSpGenerator(sSchema1,not_exist_pid_list,gDbIspectConnection1,aMeshBoxset)

	sSchema2 = DB_SCHEMA2
	OIDRIDMap2,TableIDRIDMap2,RIDOIDMap2,OIDtoRIDCnt2 = GetSpGenerator(sSchema2,not_exist_pid_list,gDbIspectConnection2,aMeshBoxset)

	# 生成先テーブルデータの取得
	OIDRIDResultRecord1,CompOIDDateMap1,OIDset1 = GetMapTableDate(not_exist_pid_list,OIDRIDMap1,TableIDRIDMap1,RIDOIDMap1,gDbIspectConnection1,sSchema1,OIDtoRIDCnt1)

	# 生成先テーブルデータの取得
	OIDRIDResultRecord2,CompOIDDateMap2,OIDset2 = GetMapTableDate(not_exist_pid_list,OIDRIDMap2,TableIDRIDMap2,RIDOIDMap2,gDbIspectConnection2,sSchema2,OIDtoRIDCnt2)
	


	return OIDRIDResultRecord1,OIDRIDResultRecord2,CompOIDDateMap1,CompOIDDateMap2,OIDset1,OIDset2


# ------------------------------------------------
# DB接続
# ------------------------------------------------
def DBConnect():
	global gDbIspectConnection1,gDbIspectConnection2,gTabaleMngDbIspectConnection
	
	# DB接続1
	gDbIspectConnection1 = pg_driver.connect(
		user      = DB_USER1,
		password  = DB_PASS1,
		database  = DB_NAME1,
		host      = DB_HOST1,
		port      = DB_PORT1)
	# DB接続2
	gDbIspectConnection2 = pg_driver.connect(
		user      = DB_USER2,
		password  = DB_PASS2,
		database  = DB_NAME2,
		host      = DB_HOST2,
		port      = DB_PORT2)

	# DB接続テーブル管理
	gTabaleMngDbIspectConnection = pg_driver.connect(
		user      = TBLMNG_USER,
		password  = TBLMNG_PASS,
		database  = TBLMNG_DB_NAME,
		host      = TBLMNG_HOST,
		port      = TBLMNG_PORT)

# ------------------------------------------------
# 地物DB接続
# ------------------------------------------------
def OBJDBConnect():
	# 地物DB接続1
	gObjDbIspectConnection1 = pg_driver.connect(
		user      = OBJ_USER,
		password  = OBJ_PASS,
		database  = OBJ_NAME,
		host      = OBJ_HOST[0],
		port      = OBJ_PORT[0])
	# 地物DB接続2
	gObjDbIspectConnection2 = pg_driver.connect(
		user      = OBJ_USER,
		password  = OBJ_PASS,
		database  = OBJ_NAME,
		host      = OBJ_HOST[1],
		port      = OBJ_PORT[1])
	# 地物DB接続3
	gObjDbIspectConnection3 = pg_driver.connect(
		user      = OBJ_USER,
		password  = OBJ_PASS,
		database  = OBJ_NAME,
		host      = OBJ_HOST[2],
		port      = OBJ_PORT[2])
	# 地物DB接続4
	gObjDbIspectConnection4 = pg_driver.connect(
		user      = OBJ_USER,
		password  = OBJ_PASS,
		database  = OBJ_NAME,
		host      = OBJ_HOST[3],
		port      = OBJ_PORT[3])
	gObjALLDbIspectConnection = [gObjDbIspectConnection1,gObjDbIspectConnection2,gObjDbIspectConnection3,gObjDbIspectConnection4]

# ------------------------------------------------
# テーブル管理とカラム管理の取得
# ------------------------------------------------
def GetTableMap():

	global gTableMap,gClmMap,gDoClmmap

	# テーブル管理取得
	sQuery = '''
    			SELECT table_id, physical_table_name, logical_table_name
  				FROM SCHEMA.tb_taf_table_mng
		    '''.replace('SCHEMA',TBLMNG_SCHEMA)

	# クエリ実行
	tablemap = {}
	aResultRecordList = gTabaleMngDbIspectConnection.query(sQuery)
	for aResultRecord in aResultRecordList:
		key = aResultRecord['table_id']
		value = (aResultRecord['physical_table_name'],aResultRecord['logical_table_name'])
		gTableMap[key] = value

	# カラム管理取得
	sQuery = '''
    			SELECT table_id, physical_column_name, logical_column_name
				FROM SCHEMA.tb_taf_column_mng;
		    '''.replace('SCHEMA',TBLMNG_SCHEMA)
	# クエリ実行
	Clmmap = {}
	aResultRecordList = gTabaleMngDbIspectConnection.query(sQuery)
	for aResultRecord in aResultRecordList:
		key = aResultRecord['table_id']
		value = (aResultRecord['physical_column_name'],aResultRecord['logical_column_name'])
		gClmMap[key] = value

	sQuery='''SELECT table_id, physical_column_name, logical_column_name ,ordinal_position
					  FROM SCHEMA.tb_taf_column_mng
					  where physical_column_name not in (
					--共通
					'create_stime','update_stime','sdiff_start_stime','sdiff_end_stime','sdiff_project_start_stime','sdiff_project_end_stime','sdiff_start_process','sdiff_end_process','sdiff_start_project','sdiff_end_project','sdiff_update_status','uid'
					  ) and (physical_column_name not like '%rid%') and physical_column_name not like '%uid%'
					  order by table_id,ordinal_position'''.replace('SCHEMA',TBLMNG_SCHEMA)
	# クエリ実行
	aResultRecordList = gTabaleMngDbIspectConnection.query(sQuery)
	for aResultRecord in aResultRecordList:
		key = aResultRecord['table_id']
		valueList = [aResultRecord['physical_column_name']]
		value = valueList[0]
		if value == 'zenrin_id':
			value ='show_zid(zenrin_id) as zid'
		insertList = []
		if key in gDoClmmap:
			insertList = gDoClmmap[key]
		insertList.append(value)
		gDoClmmap[key] = insertList
	



# ------------------------------------------------
# 比較データ作成
# ------------------------------------------------
def MakeCompdate(difrecord,OIDdif):
	
	datamap = {}
	NotComp = {}
	
	for record in difrecord:
		ValueList = str(record).split(" , ")
		Tempoid = ValueList.pop(1)
		oid = Tempoid.replace('OID:','')
		# 比較対象外のOIDはスキップ
		if oid in OIDdif:
			insertnot = []
			if oid in NotComp:
				insertnot = NotComp[oid]
			insertnot.append(ValueList)
#			print(str(insertnot) + "\n")
			NotComp[oid] = insertnot
			continue
		insert = []
		TempinsertListList = []
		if oid in datamap:
			TempinsertListList = datamap[oid]
		TempinsertListList.append(ValueList)
		datamap[oid] = TempinsertListList
	return datamap,NotComp

# ------------------------------------------------
# 値の比較
# ------------------------------------------------
def CompareValue(ValueList1,ValueList2):

	outValueList = []
	index = 0
	for Value1 in ValueList1:
		if Value1 not in ValueList2:
#			outValueList2.append(outValueList2[index])
			outValueList.append(Value1)
#		index = index + 1

	return outValueList

# ------------------------------------------------
# レコードの比較
# ------------------------------------------------
def CompareResulRecord(OIDRIDResultRecord1,OIDRIDResultRecord2,CompOIDDateset1,CompOIDDateset2,OIDset1,OIDset2):


	NotExistOID = set()
	# --差異レコードの特定--
	# DB1のみに存在_レコード単位
	dif1 = CompOIDDateset1 - CompOIDDateset2
	# DB2のみに存在
	dif2 = CompOIDDateset2 - CompOIDDateset1

	# 完全に片方にしかいないOIDの特定
	# DB1のみに存在するOID
	OIDdif1 = OIDset1 - OIDset2
	# DB2のみに存在するOID
	OIDdif2 = OIDset2 - OIDset1

	# 比較データ作成 difから片方のみに存在するOIDを削除
	dif1,NotComp1 = MakeCompdate(dif1,OIDdif1)
	dif2,NotComp2 = MakeCompdate(dif2,OIDdif2)

	# 差異属性名の特定
	outOIDValueListMap1 = {}
	outOIDValueListMap2 = {}
#	print(dif2)
#	print(CompOIDDateset1)
#	print("------")
#	print(CompOIDDateset2)
#	sys.exit()
	outValueList1 = []
	outValueList2 = []
	for oid1,ValueListList1 in dif1.items():
		if oid1 in dif2:
			ValueListList2 = dif2[oid1]
		else:
			ValueListList2 = CompOIDDateset2[oid1]
			print("★")
			print(ValueList2)
#警告
			sys.exit()
		outValueListList1 = []
		outValueListList2 = []
		# 比較は先勝ちでのみ
		print(oid1)
		for ValueList1 in ValueListList1:
			outValueList1 = CompareValue(ValueList1,ValueListList2[0])
			outValueListList1.append(outValueList1)
		# 比較は先勝ちでのみ
		for ValueList2 in ValueListList2:
			outValueList2 = CompareValue(ValueList2,ValueListList1[0])
			outValueListList2.append(outValueList2)
		outOIDValueListMap1[oid1] = outValueListList1
		outOIDValueListMap2[oid1] = outValueListList2
		del dif2[oid1]

#警告
	if len(dif2) != 0:
		print("ロジックおかしい")
		pass
	DB1OnlyResultRecordListMap1 = {}
	DB2OnlyResultRecordListMap2 = {}
	# DB1のみに存在するレコード
	for oid1 in OIDdif1:
		DB1OnlyResultRecordListMap1[oid1] = NotComp1[oid1]
	# DB2のみに存在するレコード
	for oid2 in OIDdif2:
		DB2OnlyResultRecordListMap2[oid2] = NotComp2[oid2]

	return outOIDValueListMap1,outOIDValueListMap2,DB1OnlyResultRecordListMap1,DB2OnlyResultRecordListMap2

# ------------------------------------------------
# 書き込み
# ------------------------------------------------
def WriteOtherDBOnly(fp,ResultRecordListList1,ResultRecordListList2,DBname,CompType):

	for genOID1, ResultRecordList1 in ResultRecordListList1.items():
		writeRecordList2 = []
		if genOID1 in ResultRecordListList2:
			writeRecordList2 = ResultRecordListList2[genOID1]
			del ResultRecordListList2[genOID1]
		else:
			Sprecord = ""
		
		fp.write(CompType+ "|" + DBname + "|" + genOID1 + "|" + str(ResultRecordList1)+ "|" +str(writeRecordList2) + "\n")

	

	return ResultRecordListList2

# ------------------------------------------------
# ファイル出力
# ------------------------------------------------
def OutPutFile(FtDB1OnlyResultRecordList1,FtDB2OnlyResultRecordList2,difFtValueListMap1,difFtValueListMap2,SpDB1OnlyResultRecordList1,SpDB2OnlyResultRecordList2,difSpValueListMap1,difSpValueListMap2):


	fp = open("output.txt","w")
	# DB1だけに存在する事象と生成元が同じ空間を出力
	DBname = DB_NAME1
	CompType = "DBOnly"
	remSpDB1Only = WriteOtherDBOnly(fp,FtDB1OnlyResultRecordList1,SpDB1OnlyResultRecordList1,DBname,CompType)

	# DB1だけに存在する空間と結びつく事象を出力
	End = WriteOtherDBOnly(fp,remSpDB1Only,FtDB1OnlyResultRecordList1,DBname,CompType)

	# DB2だけに存在する事象と生成元が同じ空間を出力
	DBname = DB_NAME2
	remSpDB2Only = WriteOtherDBOnly(fp,FtDB2OnlyResultRecordList2,SpDB2OnlyResultRecordList2,DBname,CompType)
	# DB2だけに存在する空間と結びつく事象を出力
	End = WriteOtherDBOnly(fp,remSpDB2Only,FtDB2OnlyResultRecordList2,DBname,CompType)

	# OIDが一致するが、生成先のレコードが一致しないレコードの出力
	DBname = DB_NAME1
	CompType = "difRecord"
	remSpDB1Only = WriteOtherDBOnly(fp,difFtValueListMap1,difSpValueListMap1,DBname,CompType)	
	End = WriteOtherDBOnly(fp,remSpDB1Only,difFtValueListMap1,DBname,CompType)

	DBname = DB_NAME2
	remSpDB2Only = WriteOtherDBOnly(fp,difFtValueListMap2,difSpValueListMap2,DBname,CompType)
	End = WriteOtherDBOnly(fp,remSpDB2Only,difFtValueListMap2,DBname,CompType)

	fp.close()

# ------------------------------------------------
# Main
# ------------------------------------------------
if __name__ == '__main__':

	if len(sys.argv) != 3:
		print('エリアファイル及びプロジェクトIDを指定してください')
		sys.exit()
	sAreaFileName = sys.argv[1]
	prjid = sys.argv[2]
	
	# 領域のボックス化
	sMeshBoxset = set()
	for cArea in open(sAreaFileName,'r',encoding = "utf-8_sig"):
		print(cArea)
		if cArea != '\n':
			sMeshCode = cArea.replace('\n','')
			aMeshBox = JikuMap.convMeshCode2JikuBox(sMeshCode)
			sMeshBox = 'box\'((' + str(aMeshBox[0][0]) + ',' + str(aMeshBox[0][1]) + '),(' + str(aMeshBox[1][0]-1) + ',' + str(aMeshBox[1][1]-1) + '))\''
			sMeshBoxset.add(sMeshBox)

	# 指定外PJIDの取得
	not_exist_pid_list = getProjectList(prjid)
	
	# DB接続
	DBConnect()
	
	# テーブル管理とカラム管理マップの取得
	GetTableMap()

	# 地物事象クエリ
	OIDRIDFtResultRecord1,OIDRIDFtResultRecord2,CompFtOIDDateMap1,CompFtOIDDateMap2,OIDFtset1,OIDFtset2 = GetFtQueryDate(not_exist_pid_list,sMeshBoxset)

	# 空間クエリ
	OIDRIDSpResultRecord1,OIDRIDSpResultRecord2,CompSpOIDDateMap1,CompSpOIDDateMap2,OIDSpset1,OIDSpset2 = GetSpQueryDate(not_exist_pid_list,sMeshBoxset)

	# 事象の比較
	dif1FtOIDMapDate, dif2FtOIDMapDate, DB1OnlyFtOIDMapDate, DB2OnlyFtOIDMapDate = CompareResulRecord(OIDRIDFtResultRecord1,OIDRIDFtResultRecord2,CompFtOIDDateMap1,CompFtOIDDateMap2,OIDFtset1,OIDFtset2)
	
	# 空間の比較
	dif1SpOIDMapDate, dif2SpOIDMapDate, DB1OnlySpOIDMapDate, DB2OnlySpOIDMapDate = CompareResulRecord(OIDRIDSpResultRecord1,OIDRIDSpResultRecord2,CompSpOIDDateMap1,CompSpOIDDateMap2,OIDSpset1,OIDSpset2)

	# ファイル出力
	OutPutFile(DB1OnlyFtOIDMapDate, DB2OnlyFtOIDMapDate, dif1FtOIDMapDate, dif2FtOIDMapDate, DB1OnlySpOIDMapDate, DB2OnlySpOIDMapDate,dif1SpOIDMapDate, dif2SpOIDMapDate)

	sys.exit()






