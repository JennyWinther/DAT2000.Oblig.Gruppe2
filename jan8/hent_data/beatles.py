from sqlalchemy import select, literal, text


class Beatles:
    def __init__(self, engine, tabellen):
        self.engine = engine
        self.tabellen = tabellen

    def finn_beatle_id(self, beatle):
        return finn_beatle_id_trygg(self.engine, self.tabellen, beatle)

def finn_beatle_id_trygg(engine, tabellen, beatle):
    stmt = select(tabellen.c.Id).where(tabellen.c.Name == literal(beatle))
    #print(stmt)
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as c:
        res = c.execute(stmt)
        for r in res:
            return r[0]
    return None

def finn_beatle_id_utrygg(engine, beatle):
    query = f"""
        SELECT tabellen."Id" FROM tabellen WHERE tabellen."Name" = '{beatle}';
        """
    print(query)
    #Vi setter autocommit her for å få til svakheten vår..
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as c:
        res = c.execute(text(query))
        if res.returns_rows:
            for r in res:
                return r[0]
    return None