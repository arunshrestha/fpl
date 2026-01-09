from etl.pipeline import run_etl

def main():
    run_etl()  # will use get_engine() which loads env automatically

if __name__ == "__main__":
    main()
