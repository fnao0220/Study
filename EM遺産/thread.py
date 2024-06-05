
スレッドを開きました。2 件のメッセージがあります。未読メッセージはありません。

コンテンツへ
Gmail でのスクリーン リーダーの使用
t-furukawa@eandm.co.jp 
5 / 27
（件名なし）
受信トレイ

古川 隆尚 <t-furukawa@eandm.co.jp>
2019年8月23日(金) 18:13
To 自分

# 標準モジュールインポート
import os
os.chdir(os.path.dirname(os.path.abspath(__file__)))
import config
import sys
import queue
import threading

# 拡張モジュールインポート
import postgresql.driver as pg_driver

# 自作モジュールインポート
import JikuMap


#########################################################
#指定したプロジェクトIDが開始時点に承認されているプロジェクトIDリストを取得する
#########################################################
def getProjectList(projectID, db_class):
 
 # データベーススキーマ
 sSchema = config.TAF_SCHEMA
 # データベースオブジェクトの配列
 oDbConnection = pg_driver.connect(
 user      = config.TAF_USER,
 password  = config.TAF_PASS,
 database  = config.TAF_DB_NAME,
 host      = config.TAF_HOST,
 port      = config.TAF_PORT)

 not_exist_pid_list = []
 # db_class = 0 (地物)
 # db_class = 1 (地図)
 # 承認状態を取得する
 # SQL文作成　※スキーマと、メッシュ番号は置換して指定する
 sQuery = '''WITH target_start_time AS (
                SELECT start_time
                      FROM SCHEMA.tb_dbm_mng_project_approval_manage
                      WHERE project_id = PID AND db_class = DBCLASS)
                SELECT project_id
                     FROM SCHEMA.tb_dbm_mng_project_approval_manage, target_start_time
                     WHERE db_class = DBCLASS
                AND (
                    (tb_dbm_mng_project_approval_manage.approval_time > target_start_time.start_time)
                OR (tb_dbm_mng_project_approval_manage.approval_time IS NULL))
                AND tb_dbm_mng_project_approval_manage.project_id NOT IN ( PID )'''.replace('SCHEMA',sSchema).replace('PID', str(projectID)).replace('DBCLASS',db_class)

 # クエリ実行
 aResultRecordList = oDbConnection.query(sQuery)
 for record in aResultRecordList:
  not_exist_pid_list.append(str(record['project_id']))
 if len(not_exist_pid_list) == 0:
  not_exist_pid_list.append(9999)

 return not_exist_pid_list

#########################################################
# 比較
#
#########################################################
def getCompResult(not_exist_objpid_list,not_exist_ftpid_list,qMeshCode,aDbConnection,aobjDbConnectionList):

 # データベースとの接続を確立する
 sSchema = config.SCHEMA
 sObjSchema = config.OBJ_SCHEMA
 # キューからメッシュ番号を取得する
 # 取得した番号がNoneの場合処理を終了する
 while True:
  sMeshCode = qMeshCode.get()
  if sMeshCode == None:
   break
#  try:
  compResult(sMeshCode, sSchema,sObjSchema, aDbConnection, aobjDbConnectionList, not_exist_objpid_list,not_exist_ftpid_list)
  print('処理成功　－－　スレッド：' + str(threading.current_thread().name) + '　処理図：' + sMeshCode)
#  except:
  print('処理失敗　－－　スレッド：' + str(threading.current_thread().name) + '　処理図：' + sMeshCode)
  # 取得した番号について終了したことを報告する
  qMeshCode.task_done()

#########################################################
# 形状情報の取得
#
# 指定されたメッシュの道路領域・交差点領域について形状情報を取得する
#
# INPUT   sMeshCode      メッシュ番号
#         sSchema        スキーマ
#         aDbConnection  データベースオブジェクトのリスト
# OUTPUT  True：正常終了
#
#########################################################
def compResult(sMeshCode, sSchema, sObjSchema, aDbConnection, aobjDbConnectionList, not_exist_objpid_list,not_exist_ftpid_list):

 # sMeshCodeの整形
 if '\n' in sMeshCode:
  sMeshCode = sMeshCode.replace('\n','')
 # メッシュのバウンダリボックスを作成し、SQLに整形
 aMeshBox = JikuMap.convMeshCode2JikuBox(sMeshCode)
 sMeshBox = 'box\'((' + str(aMeshBox[0][0]) + ',' + str(aMeshBox[0][1]) + '),(' + str(aMeshBox[1][0]-1) + ',' + str(aMeshBox[1][1]-1) + '))\''
 if len(not_exist_objpid_list) != 1:
    not_exist_objpid_str = ",".join(not_exist_objpid_list)
 else:
    not_exist_objpid_str = str(not_exist_objpid_list[0])

 if len(not_exist_ftpid_list) != 1:
    not_exist_ftpid_str = ",".join(not_exist_ftpid_list)
 else:
    not_exist_ftpid_str = str(not_exist_ftpid_list[0])

 # -------------------------------------------地物側:領域内の道路領域取得-------------------------------------------
 # 地物の道路領域のZIDと領域取得
 sQuery = '''
    SELECT show_zid(zenrin_id), show_wk_pntary(mdl_wk_sp_attr_vdata)
     FROM SCHEMA.tb_dbm_obj_ftr_ft
    WHERE ct_cd = 2049 AND (sdiff_start_project not in (PID_LIST) AND (sdiff_end_project is null or sdiff_end_project in (PID_LIST))) AND box_mbr && BOX
    '''.replace('SCHEMA',sObjSchema).replace('PID_LIST',not_exist_objpid_str).replace('BOX',sMeshBox)
 # クエリ実行
 aResultRecordList = []
 zidmap = {}
 tempAreaSet = set() 
 zidset = set()
 for Dbcon in aobjDbConnectionList:
  aTempResultRecordList = Dbcon.query(sQuery)
  # 分散DB
  for ResultRecord in aTempResultRecordList:
   tempAreaSet = set()
   ZID = ResultRecord[0]
   AREA = ResultRecord[1]
   if ZID in zidmap:
    tempAreaSet = zidmap[ZID]
    tempAreaSet.add(AREA)
   else:
    tempAreaSet.add(AREA)
   zidmap[ZID] = tempAreaSet
   zidset.add(ZID)
 print(AREA)
 # -------------------------------------------地図側：道路領域取得-------------------------------------------
 # 領域内道路取得
 sQuery = '''
    SELECT rid, show_zid(zenrin_id),show_area(t_llx_hbr, t_lly_hbr)
     FROM SCHEMA.tb_cgh_ft_road_area
    where t_box_hbr && BOX AND (sdiff_start_project not in (PID_LIST) AND (sdiff_end_project is null or sdiff_end_project in (PID_LIST)))'''.replace('SCHEMA',sSchema).replace('PID_LIST',not_exist_ftpid_str).replace('BOX',sMeshBox)
 # クエリ実行
 aResultRecordList = aDbConnection.query(sQuery)
 # ridキーに領域とZIDのマップ作成
 RIDList = []
 TempZIDAreaList = []
 RIDArea = {}
 RIDZID = {}
 for aResultRecord  in aResultRecordList:
  TempAreZIDList=[]
  ZID = aResultRecord[1]
  LLAREA = aResultRecord[2]
  RIDArea[aResultRecord['rid']] = LLAREA
  RIDZID[aResultRecord['rid']] = ZID
  RIDList.append(aResultRecord['rid'])
 
 strRIDList = str(RIDList).strip("[").strip("]")
 # 地図側：RIDから形状取得
 sQuery = '''
   SELECT tb_cgh_df_spmng_ftomng_rlt.t_represent_rid_1, tb_cgh_df_spmng_ftomng_rlt.shape_mng_rid, tb_cgh_sp_shape_polygon.rid, bytea2coords('polygon', tb_cgh_sp_shape_polygon.coordinate)
     FROM SCHEMA.tb_cgh_df_spmng_ftomng_rlt
   INNER JOIN SCHEMA.tb_cgh_sp_shape_polygon
   ON tb_cgh_df_spmng_ftomng_rlt.shape_mng_rid = tb_cgh_sp_shape_polygon.t_represent_rid
   WHERE
     (tb_cgh_df_spmng_ftomng_rlt.sdiff_start_project not in (PID_LIST) AND (tb_cgh_df_spmng_ftomng_rlt.sdiff_end_project is null or tb_cgh_df_spmng_ftomng_rlt.sdiff_end_project in (PID_LIST)))
     and
     (tb_cgh_sp_shape_polygon.sdiff_start_project not in (PID_LIST) AND (tb_cgh_sp_shape_polygon.sdiff_end_project is null or tb_cgh_sp_shape_polygon.sdiff_end_project in (PID_LIST)))
    AND tb_cgh_df_spmng_ftomng_rlt.t_represent_rid_1 IN( RID )'''.replace('SCHEMA',sSchema).replace('RID',  strRIDList ).replace('PID_LIST',not_exist_ftpid_str)
 # クエリ実行
 aResultRecordList = aDbConnection.query(sQuery)
 tempAreaSet = set() 
 mapZIDAREA = {}
 for aResultRecord  in aResultRecordList:
  rid = aResultRecord[0]
  roadArea = aResultRecord[3]
  roadArea = str(roadArea)
  roadArea = roadArea.strip("postgresql.types.Array")
  roadArea = str(roadArea).replace("[","").replace("]","").replace("', '","),(").replace("'","")
#  print(roadArea)
  roadZID = RIDZID[rid]
  if roadZID in mapZIDAREA:
   tempAreaSet = mapZIDAREA[ZID]
   tempAreaSet.add(roadArea)
  else:
   tempAreaSet.add(roadArea)
  mapZIDAREA[ZID] = tempAreaSet





 return True

#########################################################
# メインスレッド
#
#########################################################
def main():
 # キューを宣言
 qMeshCode = queue.Queue()

 # 引数を確認（エリアファイルとPJIDを指定するため引数は2個必要）
 if len(sys.argv) == 4:
  sAreaFileName = sys.argv[1]
  ObjPJID = sys.argv[2]
  FTPJID = sys.argv[3]
 else:
  print(4, 'エリアファイル及びプロジェクトIDを指定してください')
  sys.exit()
 # メッシュコードリスト読み込み
 aMeshCodeList = [sMeshCode.replace('\n','') for sMeshCode in open(sAreaFileName,'r') if sMeshCode != '\n']

 # 対象外PJID取得
 db_class = 0
 not_exist_objpid_list =  getProjectList(ObjPJID,str(db_class))
 db_class = 1
 not_exist_ftpid_list  =  getProjectList(FTPJID,str(db_class))

 # スレッド格納用リスト作成
 aThreadList = []

 # スレッド数 ※メモリ使用量も考えて5とする　必要に応じて変更する
 iThreadNum = 1

 # 地図データベースオブジェクトの配列
 aDbConnection = pg_driver.connect(
 user      = config.USER,
 password  = config.PASS,
 database  = config.DB_NAME,
 host      = config.HOST,
 port      = config.PORT)

 # 地物データベースオブジェクトの配列
 # 分散数
 dividenum = 4
 aobjDbConnectionList = []
 for i in range(1,dividenum):
  aobjDbConnectionList.append(pg_driver.connect(
  user      = config.OBJ_USER,
  password  = config.OBJ_PASS,
  database  = config.OBJ_DB_NAME + "_" + str(i),
  host      = config.OBJ_HOST,
  port      = config.OBJ_PORT))

 

 # スレッドを起動
 for i in range(iThreadNum):
  oThread = threading.Thread(target=getCompResult,args=(not_exist_objpid_list,not_exist_ftpid_list,qMeshCode,aDbConnection,aobjDbConnectionList,))
  oThread.start()
  aThreadList.append(oThread)

 # キューにメッシュ番号を格納
 for sMeshCode in aMeshCodeList:
  qMeshCode.put(sMeshCode)
 # キューにたまったメッシュがすべて処理されるまで待つ
 qMeshCode.join()

 print('指定されたエリアのShapefileを作成し終わりました。')
 print('スレッドを停止します。')

 for i in range(iThreadNum):
  qMeshCode.put(None)
 # 作成したスレッドを停止する（Noneを停止信号として利用している）
 for oThread in aThreadList:
  oThread.join()
 print('すべてのスレッドを正常に停止しました。')

 print('Shapefileの作成を開始します。')

if __name__ == '__main__':
    main()



...

[メッセージの一部が表示されています]  メッセージ全体を表示

akasaa akasa <fnao0220@gmail.com>
2019年8月26日(月) 0:41
To 古川


 zidobjShapemap = {}
 tempAreaSet = set()
 zidset = set()
 for Dbcon in aobjDbConnectionList:
  aTempResultRecordList = Dbcon.query(sQuery)
  # 分散DB
  for ResultRecord in aTempResultRecordList:
   tempAreaSet = set()
   ZID = ResultRecord[0]
   AREA = ResultRecord[1]
   if ZID in zidobjShapemap:
    tempAreaSet = zidobjShapemap[ZID]
    tempAreaSet.add(AREA)
   else:
    tempAreaSet.add(AREA)
   zidobjShapemap[ZID] = tempAreaSet
   zidset.add(ZID)
 print(AREA)
 # -------------------------------------------地図側：道路領域取得-------------------------------------------
 # 領域内道路取得
 sQuery = '''
    SELECT rid, show_zid(zenrin_id),show_area(t_llx_hbr, t_lly_hbr)
     FROM SCHEMA.tb_cgh_ft_road_area
    where t_box_hbr && BOX AND (sdiff_start_project not in (PID_LIST) AND (sdiff_end_project is null or sdiff_end_project in (PID_LIST)))'''.replace('SCHEMA',sSchema).replace('PID_LIST',not_exist_ftpid_str).replace('BOX',sMeshBox)
 # クエリ実行
 aResultRecordList = aDbConnection.query(sQuery)
 # ridキーに領域とZIDのマップ作成
 RIDList = []
 TempZIDAreaList = []
 RIDArea = {}
 RIDZID = {}
 for aResultRecord  in aResultRecordList:
  TempAreZIDList=[]
  ZID = aResultRecord[1]
  LLAREA = aResultRecord[2]
  RIDArea[aResultRecord['rid']] = LLAREA
  RIDZID[aResultRecord['rid']] = ZID
  ZIDRID[ZID] = rid
  RIDList.append(aResultRecord['rid'])
 
 strRIDList = str(RIDList).strip("[").strip("]")
 # 地図側：RIDから形状取得
 sQuery = '''
   SELECT tb_cgh_df_spmng_ftomng_rlt.t_represent_rid_1, tb_cgh_df_spmng_ftomng_rlt.shape_mng_rid, tb_cgh_sp_shape_polygon.rid, bytea2coords('polygon', tb_cgh_sp_shape_polygon.coordinate)
     FROM SCHEMA.tb_cgh_df_spmng_ftomng_rlt
   INNER JOIN SCHEMA.tb_cgh_sp_shape_polygon
   ON tb_cgh_df_spmng_ftomng_rlt.shape_mng_rid = tb_cgh_sp_shape_polygon.t_represent_rid
   WHERE
     (tb_cgh_df_spmng_ftomng_rlt.sdiff_start_project not in (PID_LIST) AND (tb_cgh_df_spmng_ftomng_rlt.sdiff_end_project is null or tb_cgh_df_spmng_ftomng_rlt.sdiff_end_project in (PID_LIST)))
     and
     (tb_cgh_sp_shape_polygon.sdiff_start_project not in (PID_LIST) AND (tb_cgh_sp_shape_polygon.sdiff_end_project is null or tb_cgh_sp_shape_polygon.sdiff_end_project in (PID_LIST)))
    AND tb_cgh_df_spmng_ftomng_rlt.t_represent_rid_1 IN( RID )'''.replace('SCHEMA',sSchema).replace('RID',  strRIDList ).replace('PID_LIST',not_exist_ftpid_str)
 # クエリ実行
 aResultRecordList = aDbConnection.query(sQuery)
 tempAreaSet = set()
 mapZIDAREA = {}
 zidftshapemap = {}
 for aResultRecord  in aResultRecordList:
  polygonlist = []
  rid = aResultRecord[0]
  if rid not in  RIDZID
     # エラー処理を入れる★
     continue
  ZID = RIDZID[rid]
  if ZID in zidftshapemap:
     temppolygon = zidftshapemap[RIDZID[rid]]
     polygonlist = temppolygon + aResultRecord[3].pop()
  else:
     polygonlist = aResultRecord[3].pop()
  zidftshapemap[ZID] = polygonlist
 
  notexitzid = []
  # 出力ステータス一覧
 
  errorlist = []
  # 領域比較
  for objzid in zidobjShapemap:
      if objzid not in zidftshapemap:
         # 地物にいるが地図に存在しない
         notexitzid.append(objzid)
         status =
         continue
      objpolygonlist = zidobjShapemap[objzid]
      ftpolygonlist = zidftshapemap[objzid]
      for objpolygon in objpolygonlist:
         if objpolygon in  ftpolygonlist:
            continue
         # 地物の形状が地図に存在しない
         status =
         errorline = str(status) + ZIDRID[objzid] + RIDArea[ZIDRID[objzid]]
         errorlist.append(errorline)
     
 
 
  return errorlist

2019年8月23日(金) 18:13 古川 隆尚 <t-furukawa@eandm.co.jp>:

output.zip を表示しています。