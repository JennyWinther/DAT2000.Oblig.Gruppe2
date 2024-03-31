#Saksa fra tutorialen her: https://fastapi.tiangolo.com/tutorial/first-steps/
from fastapi import FastAPI, Query
from kjoretoy import kjoretoy_tabell
from dotenv import load_dotenv
from sqlalchemy import create_engine, literal
import os

load_dotenv()
connstr = os.environ.get("CONN")
if connstr is None:
    connstr = "postgresql+psycopg2://postgres:mysecretpassword@localhost/postgres"

kjoretoy = kjoretoy_tabell()
engine = create_engine(connstr)
app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/regdato/{regdatum}")
async def regdato(regdatum: str):

    with engine.connect() as conn:
        res = conn.execute(
            kjoretoy.select().with_only_columns(
                kjoretoy.c.tekn_reg_f_g_n,
                kjoretoy.c.farge_navn,
                kjoretoy.c.tekn_modell,
                kjoretoy.c.merke_navn,
                kjoretoy.c.elbil
            ).where(
                kjoretoy.c.tekn_reg_f_g_n == literal(regdatum))
        )

        out_list = []
        for r in res:
            out = {}
            out["regdato"] = r[0]
            out["farge"] = r[1]
            out["modell"] = r[2]
            out["merke"] = r[3]
            out["elbil"] = r[4]
            out_list.append(out)
        return out_list
#20241229
@app.get("/pkkdato/{dato}")
async def pkkdato(dato: str):
    with engine.connect() as conn:
        res = conn.execute(
            kjoretoy.select().with_only_columns(
                kjoretoy.c.tekn_modell,
                kjoretoy.c.merke_navn,
            ).where(
                kjoretoy.c.tekn_neste_pkk == literal(dato))
        )
        out_list = []
        for r in res:
            out = {}
            out["modell"] = r[0]
            out["merke"] = r[1]
            out_list.append(out)
        return out_list