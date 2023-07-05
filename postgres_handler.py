import json
import psycopg2
from psycopg2.extras import RealDictCursor

class Db:
    def __init__(self):
        self.conn = psycopg2.connect(
            host="localhost",
            database="sport",
            user="robzeeman",
            password="bonzo"
        )
        self.item = []

    def version(self):
        cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        print('PostgreSQL database version:')
        cur.execute('SELECT version()')

        # display the PostgreSQL database server version
        db_version = cur.fetchone()
        cur.close()
        return {"version": db_version}

    def detail(self, id):
        cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM vereniging_vereniging WHERE id = " + id)
        record = cur.fetchone()
        self.single_field(record, "naam", "Naam")
        self.single_field(record, "opm_naam", "Opmerkingen naam")
        self.single_field(record, "plaats", "Plaats")
        self.single_field(record, "doelstelling", "Doelstelling")
        self.single_field(record, "levensbeschouwing", "Levensbeschouwing")
        self.date_fields(record, "begindatum", record["begindatum_soort"], "Begindatum")
        self.date_fields(record, "einddatum", record["einddatum_soort"], "Einddatum")
        self.single_field(record, "verantwoording_gegevens", "Verantwoording gegevens")
        self.single_field(record, "oprichters", "Oprichters")
        self.single_field(record, "bestuursleden", "Bestuursleden")
        cur.close()
        retVal = self.item.copy()
        self.item = []
        return retVal

    def single_field(self, item, key, label):
        if (item[key] != None and item[key] != ""):
            self.item.append({"field": key, "value": item[key], "label": label})
        return

    def date_fields(self, item, key, typeDate, label):
        if (item[key] != None and str(item[key]) != ""):
            self.item.append({"field": key, "value": str(item[key]) + " (" + typeDate + ")", "label": label})
        return

    def get_provincie(self):
        pass


