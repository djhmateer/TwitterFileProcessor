using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TwitterFileProcessor
{
    public static class Util
    {
        public static IDbConnection GetOpenConnection()
        {
            var connection = new SqlConnection(@"Data Source=.\;initial catalog=SimpleTwitter;integrated security=True;MultipleActiveResultSets=True;");
            connection.Open();
            //MiniProfiler.Settings.SqlFormatter = new StackExchange.Profiling.SqlFormatters.SqlServerFormatter();
            //return new ProfiledDbConnection(connection, MiniProfiler.Current);
            return connection;
        }
    }
}
