using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace TwitterFileProcessor
{
    class Program
    {
        static void Main()
        {
            var sw = new Stopwatch();
            sw.Start();

            var files = Directory.GetFiles(@"c:\Tweets\split\", "*.json");
            int countOfTweetsProcessed = 0;

            foreach (var file in files)
            {
                Console.WriteLine(file);
                // Load the file (Extract)
                var jsonLines = File.ReadAllLines(file).ToList();

                // take each line which is a Tweet
                foreach (var json in jsonLines)
                {
                    Tweet tweet;
                    try
                    {
                        // deserialise
                        tweet = DeserialiseJsonToTweet(json);
                    }
                    catch (Exception)
                    {
                        continue; // to next tweet
                    }

                    // Do business logic ie split into what we want for the Database (Transform)

                    // Load (Insert into the DB)
                    // Save to MSSQL
                    // Insert into MSSQL db using Dapper - see Util.cs for connection string
                    using (var db = Util.GetOpenConnection())
                    {
                        // have we seen this user before, and if yes what is our ID?
                        // very possible to have dupes here
                        int userID = db.Query<int>("SELECT TOP 1 UserID FROM [Users] WHERE UserIDFromTwitter = @UserIDFromTwitter ORDER BY UserID DESC", new { UserIDFromTwitter = tweet.user.id }).FirstOrDefault();
                        if (userID == 0)
                        {
                            // insert new user
                            var sql = @"
                            INSERT INTO [Users](Name, UserIDFromTwitter) VALUES (@Name, @UserIDFromTwitter)
                            SELECT CAST(SCOPE_IDENTITY() as int)";
                            userID = db.Query<int>(sql, new { Name = tweet.user.name, UserIDFromTwitter = tweet.user.id }).Single();
                        }

                        // Insert LanguageID (if not there already)

                        // Insert HashTags (if not there already)

                        db.Execute(@"INSERT INTO Tweets(Text, TweetIDFromTwitter, UserID) VALUES (@Text, @TweetIDFromTwitter, @UserID)",
                            new { Text = tweet.text, TweetIDFromTwitter = tweet.id, UserID = userID });
                    }

                    countOfTweetsProcessed++;
                    if (countOfTweetsProcessed % 5000 == 0)
                    {
                        var tweetsPerSec = countOfTweetsProcessed / (sw.ElapsedMilliseconds/1000);
                        Console.WriteLine($"So far {tweetsPerSec} t/s");
                    }

                }
                // get a timing of t/s (after each file so it includes the file loading etc..)
            }

            sw.Stop();
            Console.WriteLine($"All Done! Waiting. Finished in {sw.ElapsedMilliseconds}");
        }

        public static Tweet DeserialiseJsonToTweet(string json)
        {
            if (string.IsNullOrEmpty(json))
                throw new ArgumentException("Cannot deserialise a null or empty string");

            //JSON.NET
            var settings = new JsonSerializerSettings();
            settings.Converters.Add(new IsoDateTimeConverter
            {
                DateTimeFormat = "ddd MMM dd HH:mm:ss +ffff yyyy",
                DateTimeStyles = DateTimeStyles.AdjustToUniversal
            });
            //When a property is found in the json that we don't have in our C# model, throw JsonSerializationException
            // this is doing both ways..very strict
            //settings.MissingMemberHandling = MissingMemberHandling.Error;
            settings.MissingMemberHandling = MissingMemberHandling.Ignore;
            // cope with ' eg hashtag: #don't in text.. it is actually don in Entities in the source
            //settings.StringEscapeHandling = StringEscapeHandling.EscapeHtml;
            var tweet = JsonConvert.DeserializeObject<Tweet>(json, settings);

            //TODO optimisation (see Manual code at bottom) 
            //http://stackoverflow.com/questions/26380184/how-to-improve-json-deserialization-speed-in-net-json-net-or-other
            //TODO try Jil? if can get datetimes

            // eg {[limit, {"track": 6,"timestamp_ms": "1488384664896"}]}
            //TODO maybe faster to not throw.. use a Maybe<T>?
            if (tweet.id_str == null)
                throw new ArgumentException("Valid json but not a valid tweet, as no ID found");

            return tweet;
        }
    }
}
