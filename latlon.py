#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Ingest 80 GB of CSV Vessel Traffic Data from MarineCadastre to 
PostgreSQL+PostGIS, create UTM zone polygons

    
*******************************************************************************************
    **************************** BEFORE EXECUTING THIS SCRIPT 
*********************************
    Download data into the same directory as this file, by doing:
    $ wget -np -r -nH -L --cut-dirs=3 
https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2017/
    
*******************************************************************************************
    
*******************************************************************************************

    Running:
    $ python latlon.py > log.txt

    This script is separated into standalone functions for each subtask. 
A 'zone' table is created and polygons
    are created as specified by the UTM Mercantor projection, 
delta-longitude 6 degree, delta-latitude 8 degrees.

    A 'vessels' table is created that ingests CSV files from 
MarineCadastre *sequentially* by downloading individual 
    zip files, copying CSV data to the vessels table, and deleting 
ingested source files. 

    Requirements:
    1. Python 3.7
    2. >100GB disk space
    3. PostgreSQL hostname, database, password
    4. Postgis extension installed and enabled

    Sources:
    1. https://marinecadastre.gov/ais/
    2. https://marinecadastre.gov/img/data-dictionary.jpg
    3. http://www.dmap.co.uk/utmworld.htm

    Guides:
    1. https://www.kevfoo.com/2012/01/Importing-CSV-to-PostGIS/

    author: Yosyp Schwab
    email: yschwab@iqt.org
    date: 2019-05-06
"""

import os
import sys
import shutil
import requests
import psycopg2
import zipfile
from progress.bar import Bar, ChargingBar, FillingCirclesBar

"""
Modify the db connection parameters below to reflect your Postgres 
server details.
"""
con = psycopg2.connect(database='yschwab', user='yschwab')

def build_zone_polygons(con):
    """Build Postgis polygons at 6/8 degree lon/lat intervals on 
mercantor projection.

        This function creates a 2D list 'queryvar' that contains the 
tile name (eg 60X) and
        the 5 points that created the closed polygon (4 sides, 4 points, 
1st=5th point).
        Each polygon is added as an individual query.
    """

    print("\n[*] Building zones to polygons:")

    llatname = ['C', 'D', 'E', 'F', 'G', 'H', 'J', 'K', 'L', 'M', 'N', 
'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W'] # 'X' mising
    llonname = list(range(1,61))
    llat = list(range(-80, 72, 8))
    llon = list(range(-180, 180, 6))
    queryvar = []

    bar = ChargingBar('[*] Preparing Queries', 
max=len(llat)*len(llon)+len(llon))

    for (lat, latname) in zip(llat, llatname):
        for (lon, lonname) in zip(llon, llonname):
            queryvar.append([str(lonname)+latname, lat,lon, lat,lon+6, 
lat+8,lon+6, lat+8,lon, lat,lon])
            bar.next()

    lat = 72
    latname = 'X'
    for (lon, lonname) in zip(llon, llonname):
            queryvar.append([str(lonname)+latname, lat,lon, lat,lon+6, 
lat+12,lon+6, lat+12,lon, lat,lon])
            bar.next()

    bar.finish()
    cur = con.cursor()

    """
    Prettyprinted SQL query:
        INSERT INTO
        zones (name, polygon) (
            SELECT
            '60X',      
            ST_MakePolygon(
                ST_AddPoint(foo.open_line, ST_StartPoint(foo.open_line))
            )
            FROM
            (
                SELECT
                ST_GeomFromText(
                    'LINESTRING(72 174, 72 180, 84 180, 84 174, 72 174)'
                ) As open_line
            ) As foo
        );
    """
    query = "INSERT INTO zones (name, polygon) (SELECT %s, 
ST_MakePolygon(ST_AddPoint(foo.open_line, ST_StartPoint(foo.open_line))) 
FROM (SELECT ST_GeomFromText('LINESTRING(%s %s, %s %s, %s %s, %s %s, %s 
%s)') As open_line) As foo);"

    bar = FillingCirclesBar('[*] Executing Queries', 
max=len(llat)*len(llon)+len(llon))
    for i in range(len(queryvar)):
        cur.execute(query, queryvar[i])
        bar.next()
    bar.finish()

    con.commit()
    print("[*] Zone polygons created!")


def create_zones_table(con):
    """Create zones table that holds Postgis polygons and a tilename.
    """

    print("\n[*] Creating zones tables:")

    cur = con.cursor()

    cur.execute("""
            CREATE TABLE zones (
            id SERIAL PRIMARY KEY,
            name VARCHAR(64),
            polygon GEOMETRY
            );""")

    con.commit()

    print("[*] Zones table created!")


def create_vessel_table(con):
    """Create vessels table that follows the data dictionary in 
[Sources#2]. Lat/Lon are also stores in Point geometry.
    """

    print("\n[*] Creating vessel table:")

    bar = ChargingBar('[*] SQL execution', max=2)

    cur = con.cursor()

    cur.execute("""
            CREATE TABLE public.vessels
            (
            gid serial NOT NULL,
            mmsi character varying(11),
            basedatetime timestamp without time zone,
            lat real,
            lon real,
            the_geom geometry,
            sog real,
            cog real,
            heading real,
            vesselname character varying(32) COLLATE 
pg_catalog."default",
            imo character varying(16) COLLATE pg_catalog."default",
            callsign character varying(8) COLLATE pg_catalog."default",
            vesseltype integer,
            status character varying(64) COLLATE pg_catalog."default",
            length real,
            width real,
            draft real,
            cargo character varying(4) COLLATE pg_catalog."default",
            CONSTRAINT vessels_pkey PRIMARY KEY (gid),
            CONSTRAINT enforce_dims_the_geom CHECK (st_ndims(the_geom) = 
2),
            CONSTRAINT enforce_geotype_geom CHECK 
(geometrytype(the_geom) = 'POINT'::text OR the_geom IS NULL),
            CONSTRAINT enforce_srid_the_geom CHECK (st_srid(the_geom) = 
4326)
            );""")
    bar.next()

    cur.execute("""CREATE INDEX vessels_the_geom_gist
                ON public.vessels USING gist
                (the_geom)
                TABLESPACE pg_default;""")
    bar.next()

    con.commit()
    bar.finish()

    print("[*] Vessel table created!")


def copy_csv_to_table(con, filename):
    """This function copies CSV file into vessels table. Specifically:

        1. Get number of rows in vessels before ingestion
        2. Ingest CSV data
        3. Build GIS Point objects from Lat/Lon 
        4. Get number of rows in vessels after ingestion
        5. Show ingestion statistics 
    """

    cur = con.cursor()
    cur.execute("SELECT COUNT(*) FROM vessels")
    con.commit()
    rows_begin = cur.fetchone()

    print("[**] vessels table has %d rows" % rows_begin[0])

    print("[**] Copying %s to table . . . . . . " % filename)
    colslist = ('mmsi', 'basedatetime', 'lat', 'lon', 'sog', 'cog', 
'heading', 'vesselname', 'imo', 'callsign' ,'vesseltype', 'status', 
'length', 'width', 'draft', 'cargo')

    cur = con.cursor()
    csvlines = ""
    with open(filename, 'r') as f:
        next(f)  # Skip the header row.
        csvlines = sum(1 for line in f)

    with open(filename, 'r') as f:
        next(f)  # Skip the header row.
        cur.copy_from(f, 'vessels', sep=',', columns=colslist, null="")

    con.commit()

    print("[**] Finished copying %s to table" % filename)

    cur = con.cursor()
    cur.execute("SELECT COUNT(*) FROM vessels")
    con.commit()
    rows_end = cur.fetchone()
    rows_ingested = rows_end[0] - rows_begin[0]

    print("[**] {}/{} rows ingested. vessels table has {} rows 
total".format(rows_ingested, csvlines, rows_end[0]))

with con:
    create_zones_table(con)
    build_zone_polygons(con)
    create_vessel_table(con)

    filename = "2017/AIS_2017_{:02d}_Zone{:02d}.zip"
    rmpath = "AIS_ASCII_by_UTM_Month"
    csvpath = 
"AIS_ASCII_by_UTM_Month/2017_v2/AIS_2017_{:02d}_Zone{:02d}.csv"
    count = 1

    """The loop block below does these things at every iteration:
        1. Extract 1 of many zip files to folder
        2. Pass directory of CSV file in folder to copy_csv_to_table()
        3. Delete extracted folder
        4. ...
        5. Profit?
    """
    for month in range(1,13): # final value: 13
        for zone in range(1,21): # final value: 21

            if zone == 12 or zone == 13: # data omits zones 12 and 13, 
skip them here
                continue

            print("\n[%d/%d] Ingestion beginning" % (count, 12*20))

            with zipfile.ZipFile(filename.format(month, zone),"r") as 
zip_ref:
                zip_ref.extractall()
                copy_csv_to_table(con, csvpath.format(month, zone))
                shutil.rmtree(rmpath)

            print('[*] Ingested data cleanup complete.')
            count = count+1

    print("[**] Building GIS geometry from lat long columns . . . . . . 
")
    print("[**] . . . . . .  this may take a while . . . . . . ")

    cur = con.cursor()
    cur.execute("""
                UPDATE vessels
                SET the_geom = ST_GeomFromText('POINT(' || LON || ' ' || 
LAT || ')',4326);
                """)
    con.commit()

    print("[**] Built GIS geometry from lat long columns")
