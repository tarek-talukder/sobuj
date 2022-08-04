using System;
using System.Data;
using System.Configuration;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.Web.Configuration;

namespace ConnectionManager
{


    public class clsConnection
    {

        private int ExecutionTimeout = 60;//seconds or 10 minutes

        private string SERVER_NAME = WebConfigurationManager.AppSettings["DBSERVER"];
        public readonly string DATABASE_NAME = WebConfigurationManager.AppSettings["DBNAME"];
        private string USER_NAME = WebConfigurationManager.AppSettings["DBUSERID"];
        private string PASSWORD = WebConfigurationManager.AppSettings["DBUSERPWD"];
        private string SERVER_TYPE = "SQlClient";
        private string strConnString = "";
        private int connectionType;
        /*
         * SQlClient=1
         * OLEDB=2
         * */
        private IDbConnection idbConnection;
        private IDbTransaction idbTransaction;



        public clsConnection()
        {

            System.Configuration.ExeConfigurationFileMap configFile = new System.Configuration.ExeConfigurationFileMap();
            configFile.ExeConfigFilename = "Web.config";
            System.Configuration.Configuration config = System.Configuration.ConfigurationManager.OpenMappedExeConfiguration(configFile, System.Configuration.ConfigurationUserLevel.None);
            System.Configuration.KeyValueConfigurationCollection settings = config.AppSettings.Settings;

            SERVER_NAME = settings["DBSERVER"].Value;
            DATABASE_NAME = settings["DBNAME"].Value;
            USER_NAME = settings["DBUSERID"].Value;
            PASSWORD = settings["DBUSERPWD"].Value;

            //constructor
            if (string.Compare(SERVER_TYPE, "SQlClient", true) == 0)
            {
                idbConnection = new SqlConnection();
                strConnString = "Data Source=" + SERVER_NAME + ";Initial Catalog=" + DATABASE_NAME + ";User ID=" + USER_NAME + ";Password=" + PASSWORD + "";
                idbConnection.ConnectionString = strConnString;
                connectionType = 1;
            }
            else if (string.Compare(SERVER_TYPE, "OLEDB", true) == 0)
            {
                //idbConnection = new OleDbConnection();
                ////strConnString = "Provider=Microsoft.Jet.OLEDB.4.0;Data Source=" + HttpContext.Current.Server.MapPath("" + DATABASE_NAME + ".mdb") + "';User Id=" + USER_NAME + ";Password=" + PASSWORD + ";";
                //strConnString = "Provider=Microsoft.Jet.OLEDB.4.0;Data Source='" + HttpContext.Current.Server.MapPath("" + DATABASE_NAME + ".mdb") + "'";
                //idbConnection.ConnectionString = strConnString;
                //connectionType = 2;
            }

            else
            {
                System.Exception ex = new Exception("Problem in Connection String. Web.config not Configured Properly...");
                throw (ex);
            }

        }//End Construction


        public void BeginTransaction()
        {
            try
            {
                //BeginTransaction(IsolationLevel.ReadCommitted);
                BeginTransaction(IsolationLevel.ReadUncommitted);
            }
            catch (Exception ex)
            {
                throw (ex);
            }
            finally
            {
            }
        }//End Function
        private void BeginTransaction(IsolationLevel eIsoLation)
        {
            try
            {
                if (idbConnection.State.ToString() == "Closed")
                {
                    idbConnection.Open();
                }
                idbTransaction = idbConnection.BeginTransaction(eIsoLation);
            }
            catch (Exception ex)
            {
                idbConnection.Close();
                idbConnection.Dispose();
                throw (ex);
            }
            finally
            {
            }
        }//End Function
        public void CommitTransaction()
        {
            try
            {
                idbTransaction.Commit();

                if (idbConnection.State.ToString() == "Open")
                {
                    idbConnection.Close();
                    idbConnection.Dispose();
                }
            }
            catch (Exception ex)
            {
                idbTransaction.Rollback();
                idbConnection.Close();
                idbConnection.Dispose();
                throw (ex);
            }
            finally
            {
            }
        }//End Function


        //Get DataSet
        public void getDataSet(string strQuery, out System.Data.DataSet dsRef)
        {
            try
            {
                dsRef = new DataSet();

                switch (connectionType)
                {
                    case 1:
                        //SQlClient
                        getDataSetUsingSqlDataAdapter(strQuery, ref dsRef);
                        break;

                    case 2:
                        //OLEDB
                        getDataSetUsingOleDbDataAdapter(strQuery, ref dsRef);
                        break;


                }//End switch
            }
            catch (System.Exception ex)
            {
                throw (ex);
            }
            finally
            {
            }
        }//End Function

        //Save DataSet
        public void SaveData(ref System.Data.DataSet dsRef)
        {
            try
            {
                switch (connectionType)
                {
                    case 1:
                        //SQlClient
                        SaveDataUsingSqlDataAdapter(ref dsRef);
                        break;

                    case 2:
                        //OLEDB
                        SaveDataUsingOleDbDataAdapter(ref dsRef);
                        break;



                }//End switch
            }
            catch (System.Exception ex)
            {
                throw (ex);
            }
            finally
            {
            }
        }//End Function

        //executeQuery
        public void executeQuery(string strQuery)
        {
            try
            {
                switch (connectionType)
                {
                    case 1:
                        //SQlClient
                        executeSqlQuery(strQuery);
                        break;

                    case 2:
                        //OLEDB
                        executeOledbQuery(strQuery);
                        break;


                }//End switch
            }
            catch (Exception ex)
            {
                throw (ex);
            }
            finally
            {
            }
        }//End Function

        //executeQuery
        public void checkConnection()
        {
            try
            {
                switch (connectionType)
                {
                    case 1:
                        //SQlClient
                        executeSqlQuery("SELECT count(*) FROM sysobjects");
                        break;

                    case 2:
                        //OLEDB
                        executeOledbQuery("");
                        break;

                }//End switch
            }
            catch (Exception ex)
            {
                throw (ex);
            }
            finally
            {
            }
        }//End Function


        #region By Connection Type
        //Getting DataSet By Connection Type
        private void getDataSetUsingSqlDataAdapter(string strQuery, ref System.Data.DataSet dsRef)
        {
            SqlDataAdapter sqlDataAdapter = null;
            try
            {
                sqlDataAdapter = new SqlDataAdapter(strQuery, (SqlConnection)idbConnection);
                sqlDataAdapter.SelectCommand.Transaction = (SqlTransaction)idbTransaction;
                sqlDataAdapter.SelectCommand.CommandTimeout = ExecutionTimeout;
                sqlDataAdapter.Fill(dsRef);
                dsRef.ExtendedProperties.Add("Query", strQuery);
            }
            catch (System.Exception ex)
            {
                idbTransaction.Rollback();
                idbConnection.Close();
                idbConnection.Dispose();
                throw (ex);
            }
            finally
            {
                sqlDataAdapter = null;
            }
        }//End Function
        private void getDataSetUsingOleDbDataAdapter(string strQuery, ref System.Data.DataSet dsRef)
        {
            OleDbDataAdapter oleDbDataAdapter = null;
            try
            {
                oleDbDataAdapter = new OleDbDataAdapter(strQuery, (OleDbConnection)idbConnection);
                oleDbDataAdapter.SelectCommand.Transaction = (OleDbTransaction)idbTransaction;
                oleDbDataAdapter.Fill(dsRef);
                dsRef.ExtendedProperties.Add("Query", strQuery);
            }
            catch (System.Exception ex)
            {
                idbTransaction.Rollback();
                idbConnection.Close();
                idbConnection.Dispose();
                throw (ex);
            }
            finally
            {
                oleDbDataAdapter = null;
            }
        }//End Function

        //Saving DataSet By Connection Type
        private void SaveDataUsingSqlDataAdapter(ref System.Data.DataSet dsRef)
        {
            SqlCommandBuilder sqlComBulder = null;
            SqlDataAdapter sqlDataAdapter = null;
            try
            {
                string strQuery = dsRef.ExtendedProperties["Query"].ToString();
                sqlDataAdapter = new SqlDataAdapter(strQuery, (SqlConnection)idbConnection);
                sqlDataAdapter.SelectCommand.Transaction = (SqlTransaction)idbTransaction;

                sqlComBulder = new SqlCommandBuilder(sqlDataAdapter);
                sqlComBulder.GetInsertCommand();
                sqlComBulder.GetUpdateCommand();
                sqlComBulder.GetDeleteCommand();

                sqlDataAdapter.Update(dsRef);//Update Occurs when call Commit()
            }
            catch (System.Exception ex)
            {
                idbTransaction.Rollback();
                idbConnection.Close();
                idbConnection.Dispose();
                throw (ex);
            }
            finally
            {
                sqlComBulder = null;
                sqlDataAdapter = null;
            }
        }//End Function
        private void SaveDataUsingOleDbDataAdapter(ref System.Data.DataSet dsRef)
        {
            OleDbCommandBuilder oleDbComBulder = null;
            OleDbDataAdapter oleDbDataAdapter = null;
            try
            {
                string strQuery = dsRef.ExtendedProperties["Query"].ToString();
                oleDbDataAdapter = new OleDbDataAdapter(strQuery, (OleDbConnection)idbConnection);
                oleDbDataAdapter.SelectCommand.Transaction = (OleDbTransaction)idbTransaction;

                oleDbComBulder = new OleDbCommandBuilder(oleDbDataAdapter);
                oleDbComBulder.GetInsertCommand();
                oleDbComBulder.GetUpdateCommand();
                oleDbComBulder.GetDeleteCommand();

                oleDbDataAdapter.Update(dsRef); //Update Occurs when call Commit()

            }
            catch (System.Exception ex)
            {
                idbTransaction.Rollback();
                idbConnection.Close();
                idbConnection.Dispose();
                throw (ex);
            }
            finally
            {
                oleDbComBulder = null;
                oleDbDataAdapter = null;
            }
        }//End Function

        //Executing Query By Connection Type
        private void executeSqlQuery(string strQuery)
        {
            SqlCommand sqlCommand = null;
            try
            {
                sqlCommand = new SqlCommand(strQuery, (SqlConnection)idbConnection, (SqlTransaction)idbTransaction);
                sqlCommand.CommandTimeout = ExecutionTimeout;
                sqlCommand.ExecuteNonQuery(); //Execute Occurs when call Commit()
            }
            catch (Exception ex)
            {
                idbTransaction.Rollback();
                idbConnection.Close();
                idbConnection.Dispose();
                throw (ex);
            }
            finally
            {
                //sqlCon = null;
                sqlCommand = null;
            }
        }//End Function
        private void executeOledbQuery(string strQuery)
        {
            OleDbCommand oleDbCommand = null;
            try
            {
                oleDbCommand = new OleDbCommand(strQuery, (OleDbConnection)idbConnection, (OleDbTransaction)idbTransaction);
                oleDbCommand.ExecuteNonQuery(); //Execute Occurs when call Commit()
            }
            catch (Exception ex)
            {
                idbTransaction.Rollback();
                idbConnection.Close();
                idbConnection.Dispose();
                throw (ex);
            }
            finally
            {
                oleDbCommand = null;
            }
        }//End Function

        #endregion By Connection Type

        #region Destructor
        ~clsConnection()
        {
            if (idbConnection.State.ToString() == "Open")
            {
                idbConnection.Close();
                idbConnection.Dispose();
            }
        }
        #endregion Destructor

    }//End Class

}//End namespace
