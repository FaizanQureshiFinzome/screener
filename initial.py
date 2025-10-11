from db.db_schema import metadata_obj
from db.db_ops import engine

if __name__ == '__main__':
    try:
        metadata_obj.create_all(engine)
        print("created")
    except Exception as e:
        print(f"Unable to connect to the DataBase please check: {e}")