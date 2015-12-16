#!/usr/bin/env python

import sys
import signal
import traceback
from optparse import OptionParser
import user
import uuid
from subprocess import Popen, PIPE
import MySQLdb
import MySQLdb.cursors
import os
from qds_sdk.qubole import Qubole
from qds_sdk.commands import *
from qds_sdk.cluster import Cluster
import uuid
import logging
import thread
import tempfile
import subprocess

logger = logging.getLogger(__file__)

"""
dbimportcli: launch sqoop import jobs on behalf of a user account

when invoked as a program, usage:
dbimportcli.py

eg: dbimportcli.py --cid <cid> <acid>"
"""
unique_id = ''

#script to import data from Mysql to s3 FS
class dbimportcli:
    
  def __init__(self, access=None, secret = None, testmode=False, db_parallelism=None, mode=None, db_table=None, db_where=None, db_columns=None, db_boundary_query=None, db_extract_query=None, db_split_column=None, hive_table=None, part_spec=None, db_user=None, db_passwd=None, db_host=None, db_port=None, db_type=None, db_name=None, api_token = None, api_url=None, fetch_size = None):
    self.temp_location = "/tmp/sqoop/"+uuid.uuid1().hex
    self.tmp_dir = tempfile.mkdtemp(prefix="/media/ephemeral0/logs"+"/sqoop")
    logger.info("Temp Directory is:" + self.tmp_dir)
    self.access = access
    self.secret = secret
    self.api_token = api_token
    self.api_url = api_url
    self.fetch_size = fetch_size
    self.redshift_sink = False
    self.__loadImportParamsFromCid(testmode, db_parallelism, mode, db_table, db_where, db_columns, db_boundary_query, db_extract_query, db_split_column, hive_table, part_spec, db_user, db_passwd, db_host, db_port, db_type, db_name)
    self.sqoop_cmd=["/usr/lib/sqoop-h2/bin/sqoop"]
    self.sqoop_cmd.extend(["import"])
    self.__addBasicOptions()
    self.__extendCmdSpecificOptions()
    Qubole.configure(api_token=api_token, api_url=api_url)
    self.cluster_label = Cluster.show(os.popen("cat /usr/lib/hustler/bin/nodeinfo_src.sh | grep cluster_id").read().split("=")[1].strip().replace('"',''))['cluster']['label'][0]

  def __getJdbcUrl(self, sink_row):
    db_type = "mysql"
    if ((sink_row['db_type'] is not None) and (sink_row['db_type'] != "")): 
      db_type = sink_row['db_type']
    if db_type == "redshift":
      db_type = "postgresql"
      self.redshift_sink = True
    jdbc_url = "jdbc:" + db_type + "://" + sink_row['db_host']
    port = self.__getPort(sink_row["port"], sink_row["db_type"])
    if ((port is not None) and (port != "")): jdbc_url = jdbc_url + ":" + str(port)
    jdbc_url = jdbc_url + "/" + sink_row['db_name']
    print("JDBC URL is: "+ jdbc_url)
    return jdbc_url    
   
  def __loadImportParamsFromCid(self, testmode, db_parallelism, mode, db_table, db_where, db_columns, db_boundary_query, db_extract_query, db_split_column, hive_table, part_spec, db_user, db_passwd, db_host, db_port, db_type, db_name):
    cmd_row = {
      "test_mode":testmode,
      "db_parallelism":db_parallelism,
      "mode":mode,
      "db_table":db_table,
      "db_where":db_where,
      "db_columns":db_columns,
      "db_boundary_query":db_boundary_query,
      "db_extract_query":db_extract_query,
      "db_split_column":db_split_column,
      "hive_table":hive_table,
      "part_spec":part_spec,
      "username":db_user,
      "password":db_passwd
    }
    
    sink_row = {
        "db_user":db_user,
        "db_passwd":db_passwd,
        "db_host":db_host,
        "port":db_port,
        "db_type":db_type,
        "db_name":db_name
        }
    self.sink_row = sink_row  
  
    logger.debug("sink_row = %s" % sink_row)

    cmd_row['jdbc_url'] = self.__getJdbcUrl(sink_row)
    self.cmd_row = cmd_row
    self.__loadPartitionParams()

  def isTableSchemaAffected(hostName,userName,password,dbName,tableName):
      con =  MySQLdb.connect(hostName, userName, password,'information_schema')
      with con:
            cur = con.cursor()
            cur.execute("DROP TABLE IF EXISTS Writers")


  
  def __loadPartitionParams(self):
    self.cmd_row["hive-partition-key"] = None
    self.cmd_row["hive-partition-value"] = None
    p = self.cmd_row["part_spec"]
    if (p and str(p).strip()):
      partVal = []
      partKey = []
      partitions = p.split('/')
      for partition in partitions:
        pN, EQ, pV = partition.partition('=')
        if pN and pN.strip() and pV and pV.strip():
          partKey.append('`'+ pN + "` STRING")
          partVal.append('`' + pN + '`="' + pV + '"')
      self.cmd_row["hive-partition-key"] = ", ".join(partKey)
      self.cmd_row["hive-partition-value"] = ", ".join(partVal)
      logger.info("Temp Directory is: " + self.tmp_dir)
      logger.info("Hive Partition keys are: " + self.cmd_row["hive-partition-key"] )
      logger.info("Hive Partition values are: " + self.cmd_row["hive-partition-value"] )

  def __getPort(self, p, type):
    if (p and str(p).strip()):
      return p
    if (type == "mysql"): 
      return "3306"
    if (type == "redshift"):
      return "5439"
    if (type == "vertica"):
      return "5433"
    if (type == "postgresql"):
      return "5432"
    return p
    
  def __extendorWithMapping(self, mappings):
    for k in mappings:
      spec = self.cmd_row.get(k)
      if (spec and str(spec).strip()):
        self.sqoop_cmd.extend(["--%s" % mappings[k], str(spec).strip()])
  
  def __addBasicOptions(self):
    #master_ip = Popen(["cat /usr/lib/hadoop/conf/core-site.xml | grep fs.default | cut -d '>' -f5 | cut -d ':' -f2 | cut -d '/' -f3"], shell = True, stdout=PIPE).communicate()[0].strip()
    #self.sqoop_cmd.extend(["-fs", "hdfs://"+master_ip+":9000"])
    #self.sqoop_cmd.extend(["-jt", master_ip+":9001"])
    self.sqoop_cmd.extend(["-D", "fs.s3n.awsAccessKeyId="+self.access])
    self.sqoop_cmd.extend(["-D", "fs.s3n.awsSecretAccessKey="+self.secret])
    self.sqoop_cmd.extend(["-D", "fs.s3.awsAccessKeyId="+self.access])
    self.sqoop_cmd.extend(["-D", "fs.s3.awsSecretAccessKey="+self.secret])
    self.sqoop_cmd.extend(["--hive-delims-replacement", '\\040'])
    self.sqoop_cmd.extend(["--target-dir", self.temp_location])
    self.sqoop_cmd.extend(["--qubole-mode"])
    self.sqoop_cmd.extend(["--hive-import"])

  def __extendCmdSpecificOptions(self):
    if self.cmd_row['test_mode']:
      self.sqoop_cmd.extend(["--test-mode"])
    if self.redshift_sink == True:
      self.sqoop_cmd.extend(["--aws-redshift"])
    mappings = {}  
    mappings["jdbc_url"] = "connect"
    mappings["username"] = "username"
    mappings["password"] = "password"
    mappings["db_parallelism"] = "num-mappers"
    mappings["db_split_column"] = "split-by"
    mappings["hive_table"] = "hive-table"
    mappings["hive-partition-key"] = "hive-partition-key"
    mappings["hive-partition-value"] = "hive-partition-value"
    mappings["hive_serde"] = "out-table-format"
    mappings["fetch_size"] = "fetch-size"
    mode = int(self.cmd_row.get("mode"))
    if (mode == 1):
      mappings["db_table"] = "table"
      mappings["db_where"] = "where"
      mappings["db_columns"] = "columns"
    elif (mode == 2):
      mappings["db_extract_query"] = "query"
      mappings["db_boundary_query"] = "boundary-query"
    else:
      logger.error("<b>Invalid mode for sqoop loader: " + str(mode) + "</b>")
      raise RuntimeError("Mode for db import command is not valid.")
    self.__extendorWithMapping(mappings)

  def __runDfsCleanup(self):
    if self.cmd_row['test_mode']:
      logger.info("Not running Cleanup in Testmode.")
    else:
      dfscleanup = ["hadoop", "dfs", "-rmr", self.temp_location]
      a=tempfile.mkstemp()
      Popen(dfscleanup, stdout=a[0], stderr=a[0] )
      s3cleanup = ["hadoop", "dfs", "-rmr", self.get_s3_loc() + self.temp_location]
      Popen(s3cleanup, stdout=a[0], stderr=a[0] )
  
  def __getHiveCliCmd(self):
    hcli = hivecli(self.conf, False, False)
    shcmd = hcli.getcmd()
    shcmd.extend(["-hiveconf", "fs.default.name=hdfs://127.0.0.1:9000"])
    shcmd.extend(["-hiveconf", "mapred.job.tracker=127.0.0.1:9001"])
    shcmd.extend(["-hiveconf", "hadoop.rpc.socket.factory.class.default=org.apache.hadoop.net.StandardSocketFactory"])
    return shcmd

  def __runCleanupScript(self):
    if self.cmd_row['test_mode']:
      logger.info("Not running Cleanup in Testmode.")
    else:
      fname = self.tmp_dir + "/" + "cleanup_hive_query.q"
      q = open(fname).read()
      cmd=HiveCommand.create(query=q, label=self.cluster_label)

  def get_s3_loc(self):
    a= os.popen("grep s3_default_db_location /usr/lib/hustler/bin/nodeinfo_src.sh").read()
    b = a.split("/")
    c = b[2:-1]
    d = "/".join(c)
    d = "s3://" + d
    return d

  
  def fixHiveQuery(self):
    orig_file = self.tmp_dir+"/hive_query.q"
    print self.tmp_dir
    f = open(orig_file)
    l = f.readlines()
    f.close()
    del l[1]
    a = l[0].strip()[:-1]
    a = a.replace("tmp", "external")
    a = a + " LOCATION '" +  self.get_s3_loc() + self.temp_location + "';"
    l[0] = a
    f = open(orig_file,"w")
    f.write('\n'.join(l))
    f.close()

  # for dbimport we cant just return a cmd to run because running db import is
  # a 2 step process.
  def execute(self):
    logger.info("Running DbImportCommand " + str(self.sqoop_cmd))
    if self.api_url is None:
      Qubole.configure(api_token=self.api_token)
    else:
      Qubole.configure(api_token=self.api_token, api_url = self.api_url)
    p = Popen(self.sqoop_cmd, cwd=self.tmp_dir)
    retCode = p.wait()
    a= os.popen("grep s3_default_db_location /usr/lib/hustler/bin/nodeinfo_src.sh").read()
    print(self.temp_location)
    print(self.get_s3_loc())
    p = Popen(["hadoop", "dfs","-cp", self.temp_location, self.get_s3_loc() + self.temp_location])
    retCode1 = p.wait()
    if retCode != 0 or retCode1 != 0:
      logger.warn("sqoop retCode = " + str(retCode))
      self.__runCleanupScript() 
      self.__runDfsCleanup()
      return(retCode or retCode1)
    else:
      logger.debug("sqoop retCode = " + str(retCode))
    retCode = 1
    if self.cmd_row['test_mode']:
      logger.debug("Not running hive in test mode.")
      retCode = 0
    else:
      logger.info("Running hive script.")
      self.fixHiveQuery()
      q = open(self.tmp_dir+"/hive_query.q").read()
      logger.info("Query is: " + q)
      cmd=HiveCommand.create(query=q, label=self.cluster_label)
      while not Command.is_done(cmd.status):
        time.sleep(5)
        cmd = Command.find(cmd.id)
        logger.info("Hive command id: " + str(cmd.id) + "status: "+ str(cmd.status))
      logger.info(cmd.status)
      if cmd.status ==  "done":
        retCode = 0
    if retCode != 0:
      self.__runCleanupScript() 
            
    self.__runDfsCleanup()
    return(retCode)

def main():
    global unique_id
    optparser = OptionParser()

    optparser.add_option("--testmode", action="store_true", dest="testmode",default=False, help="Run in test mode")
    optparser.add_option("--db_parallelism", dest="db_parallelism",default=None, help="Db Parallelism to use")
    optparser.add_option("--mode", dest="mode",default=None, help="Mode to use")
    optparser.add_option("--db_table", dest="db_table",default=None, help="dbtable to use in simple mode")
    optparser.add_option("--db_where", dest="db_where",default=None, help="where clause to use in simple mode")
    optparser.add_option("--db_columns", dest="db_columns",default=None, help="columns to use in simple mode")
    optparser.add_option("--db_boundary_query", dest="db_boundary_query",default=None, help="boundary query to use in advanced mode")
    optparser.add_option("--db_extract_query", dest="db_extract_query",default=None, help="extract query clause to use in advanced mode")
    optparser.add_option("--db_split_column", dest="db_split_column",default=None, help="cplit column clause to use in advanced mode")
    optparser.add_option("--hive_table", dest="hive_table",default=None, help="hive_table to import data in")
    optparser.add_option("--part_spec", dest="part_spec",default=None, help="Partition to import data into")

    optparser.add_option("--db_user", dest="db_user",default=None, help="Username of database")
    optparser.add_option("--db_passwd", dest="db_passwd",default=None, help="Password of database")
    optparser.add_option("--db_host", dest="db_host",default=None, help="Hostname of database")
    optparser.add_option("--db_port", dest="db_port",default=None, help="Port of database")
    optparser.add_option("--db_type", dest="db_type",default=None, help="Type of database")
    optparser.add_option("--db_name", dest="db_name",default=None, help="Name of database")
    
    optparser.add_option("--access", dest="access",default=None, help="Aws access key")
    optparser.add_option("--secret", dest="secret",default=None, help="Aws secret key")
    optparser.add_option("--api-token", dest="api_token",default=None, help="Qubole Api Token")
    optparser.add_option("--api-url", dest="api_url",default=None, help="Qubole Api URL")
    optparser.add_option("--fetch-size", dest="fetch_size",default=None, help="Fetch Size")
     
    (options, args) = optparser.parse_args()

    logger.debug("options = " + str(options))
    logger.debug("args = " + str(args))
    status = subprocess.call(
        ['python', 'index_es.py', '--tenant=OLA', '--category=QuboleJobs', '--id=' + str(unique_id), '--src=' + str(options.db_name),
         '--job_name=' + str(options.db_table), '--create', '--status=RUNNING', '--start','--job_desc='+str(options.hive_table)+'#'+str(options.db_extract_query)])

    if (status == 1):
        logger.warn("Failed to post data to es, subprocess returned with code: " + str(status))

    importcli = dbimportcli(options.access, options.secret, options.testmode, options.db_parallelism, options.mode, options.db_table, options.db_where, options.db_columns, options.db_boundary_query, options.db_extract_query, options.db_split_column, options.hive_table, options.part_spec, options.db_user, options.db_passwd, options.db_host, options.db_port, options.db_type, options.db_name, options.api_token,options.api_url, options.fetch_size)
    retval = importcli.execute()
    if retval == 1:
        raise Exception("Failed to Import table " + options.db_table)
    else:
        status = subprocess.call(['python','index_es.py', '--id=' + str(unique_id), '--status=SUCCESS', '--end'])
        if status == 1:
            logger.warn("Failed to post data to es, subprocess returned with code: " + str(status))

    def signal_handler(*args):
        print "Waiting for JVM to terminate ..."    

    signal.signal(signal.SIGINT, signal_handler) # Or whatever signal
    return retval

if __name__ == '__main__':
    global unique_id
    try:
        unique_id = uuid.uuid4()
        code = main()
        if code != 0:
           status = subprocess.call(['python','index_es.py', '--id=' + str(unique_id), '--status=FAILED', '--end', '--err_code=403',
                                  '--err_desc=Something went wrong'])
           if (status == 1):
              logger.warn("Failed to post data to es, subprocess returned with code: " + str(status))
        sys.exit(code)
    except Exception:
        traceback.print_exc(file=sys.stderr)
        status = subprocess.call(['python','index_es.py', '--id=' + str(unique_id), '--status=FAILED', '--end', '--err_code=403',
                                  '--err_desc=Something went wrong'])
        if (status == 1):
            logger.warn("Failed to post data to es, subprocess returned with code: " + str(status))
        sys.exit(1)





