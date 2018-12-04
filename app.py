#!/usr/bin/env python
# -*- coding: utf-8 -*-

#Flask読み込み
from flask import Flask, jsonify, abort, make_response, request
from flask_cors import CORS

#SQLAlchemy読み込み
from sqlalchemy import create_engine
from sqlalchemy.event import listen
from sqlalchemy.sql import select, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import sessionmaker

#GeoAlchemy2読み込み
from geoalchemy2 import Geometry
from geoalchemy2 import WKTElement

#urllib読み込み
import urllib.request
import urllib.parse

#SpatiaLite反映
def load_spatialite(dbapi_conn, connection_record):
    dbapi_conn.enable_load_extension(True)
    #Mac環境
    dbapi_conn.load_extension('mod_spatialite.dylib')
    #AWS環境
    #dbapi_conn.load_extension('/var/task/mod_spatialite.so')

#SpatiaLite読み込み
engine = create_engine('sqlite:///address.db', echo=True)
listen(engine, 'connect', load_spatialite)

#DBに接続
conn = engine.connect()
conn.execute(select([func.InitSpatialMetaData()]))
Base = declarative_base()

#addressテーブルのModel作成
class Address(Base):
    __tablename__ = 'address'
    ogc_fid = Column(Integer, primary_key=True)
    address_all = Column(String)
    geometry = Column(Geometry(geometry_type='POINT', management=True, use_st_prefix=False, srid=4326))

#セッション作成
Session = sessionmaker(bind=engine)
session = Session()

#Flaskのインスタンス作成
app = Flask(__name__)
CORS(app)

#日本語表示対応
app.config['JSON_AS_ASCII'] = False

#JSON取得処理
@app.route('/', methods=['GET'])
def get_m():
    #URLのKeyとDBのKeyを比較
    try:
        #クエリパラメータを設定
        lng_add = request.args.get('lng', default = "", type = str)
        lat_add = request.args.get('lat', default = "", type = str)

        #クエリパラメータを判断
        if lng_add == "" or lat_add == "":
            #エラーJSON作成
            result = {
                "error": "クエリパラメータを設定してください。",
                "result":False
                }
        else:
            #バッファ(約50m)
            query = session.query(Address.ogc_fid, Address.address_all, Address.geometry.ST_X().label('lng'), Address.geometry.ST_Y().label('lat'), Address.geometry.ST_AsText().label('wkt')).filter(Address.geometry.ST_Intersects(func.ST_Buffer(WKTElement("POINT (" + lng_add + " " + lat_add + ")"), 0.0005)))

            #変数初期化
            result = {}
            count = 0
            #検索結果でJSON作成
            for m in query:
                count = count + 1
                result[str(count)] = {
                    "data":{
                        "id":m.ogc_fid,
                        "address_all":m.address_all,
                        "lng":m.lng,
                        "lat":m.lat,
                        "wkt":m.wkt
                        },
                    "result":True
                    }
                #結果は100件まで
                if count == 100:
                    break
            #最後にカウントをJSONに追加
            result["count"] = count

        #JSONを出力
        return make_response(jsonify(result))

    except Address.DoesNotExist:
        abort(404)

#エラー処理
@app.errorhandler(404)
def not_found(error):
    #エラーJSON作成
    result = {
        "error": "存在しません。",
        "result":False
        }
    #エラーJSONを出力
    return make_response(jsonify(result), 404)

#app実行
if __name__ == '__main__':
    app.run()
