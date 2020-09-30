from executioner import Executioner
from pprint import pprint
from loguru import logger


# s = executioner.Executioner("./config/inventory.yaml")
s = Executioner("./config/inventory.yaml", hostname=None, group="oracle", leave_artifacts=True)
logger.info("Test - initialized")
res = s.runSQLwithResults('manjaro_oracle', "XE", "select table_name, partitioning_type, partition_count from dba_part_tables where owner = ''TEST'' ")
s.close()


res =[[val.lstrip() for val in row] for row in res]


for item in res:
    print(item)