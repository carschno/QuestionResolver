using MongoDB.Driver.Linq;
using MongoDB.Driver;
using MongoDB.Driver.Builders;
using RestSharp;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace GoogleRequest
{
    public class GoogleResponse
    {
        public string _id;  // TODO: change to int
        public string kind { get; set; }
        public Url url { get; set; }
        public Queries queries { get; set; }
        public Context context { get; set; }
        public SearchInformation searchInformation { get; set; }
        public List<Item> items { get; set; }
    }

    public class Url
    {
        public string type { get; set; }
        public string template { get; set; }
    }

    public class NextPage
    {
        public string title { get; set; }
        public string totalResults { get; set; }
        public string searchTerms { get; set; }
        public int count { get; set; }
        public int startIndex { get; set; }
        public string inputEncoding { get; set; }
        public string outputEncoding { get; set; }
        public string safe { get; set; }
        public string cx { get; set; }
    }

    public class Request
    {
        public string title { get; set; }
        public string totalResults { get; set; }
        public string searchTerms { get; set; }
        public int count { get; set; }
        public int startIndex { get; set; }
        public string inputEncoding { get; set; }
        public string outputEncoding { get; set; }
        public string safe { get; set; }
        public string cx { get; set; }
    }

    public class Queries
    {
        public List<NextPage> nextPage { get; set; }
        public List<Request> request { get; set; }
    }

    public class Context
    {
        public string title { get; set; }
    }

    public class SearchInformation
    {
        public double searchTime { get; set; }
        public string formattedSearchTime { get; set; }
        public string totalResults { get; set; }
        public string formattedTotalResults { get; set; }
    }

    public class Metatag
    {
        public string author { get; set; }
        public string publisher { get; set; }
        public string copyright { get; set; }
        public string language { get; set; }
        public string date { get; set; }
        public string audience { get; set; }
        public string referrer { get; set; }
        public string viewport { get; set; }
    }

    public class CseImage
    {
        public string src { get; set; }
    }

    public class CseThumbnail
    {
        public string width { get; set; }
        public string height { get; set; }
        public string src { get; set; }
    }

    public class Organization
    {
        public string name { get; set; }
        public string url { get; set; }
    }

    public class Pagemap
    {
        public List<Metatag> metatags { get; set; }
        public List<CseImage> cse_image { get; set; }
        public List<CseThumbnail> cse_thumbnail { get; set; }
        public List<Organization> organization { get; set; }
    }

    public class Item
    {
        public string kind { get; set; }
        public string title { get; set; }
        public string htmlTitle { get; set; }
        public string link { get; set; }
        public string displayLink { get; set; }
        public string snippet { get; set; }
        public string htmlSnippet { get; set; }
        public string cacheId { get; set; }
        public string formattedUrl { get; set; }
        public string htmlFormattedUrl { get; set; }
        public Pagemap pagemap { get; set; }
    }

    public class GoogleQuery
    {
        private string key = "";
        private string cx = "";
        private string path = "customsearch/v1/";
        private const int defaultStart = 1;
        private static string dbname = "cache";
        private static string collectionname = "google";
        MongoCollection mongocollection;
        RestClient client;

        public int requestCount { get; set; }

        public GoogleQuery()
        {
            client = new RestClient("https://www.googleapis.com");
            requestCount = 0;
            var mongoclient = new MongoDB.Driver.MongoClient();
            var server = mongoclient.GetServer();
            var database = server.GetDatabase(dbname);
            mongocollection = database.GetCollection<GoogleResponse>(collectionname);
        }

        /// <summary>
        /// Return the number of matches for the given query.
        /// If the query is not yet available in cache, a Google API call is made.
        /// </summary>
        /// <param name="query"></param>
        /// <returns></returns>
        public long getCount(string query)
        {
            long count = 0;
            var response = makeRequest(query);

            if (response != null)
            { count = Convert.ToInt64(response.searchInformation.totalResults); }
            else
            { Debug.WriteLine("Unable to retrieve counts for query " + query); }
            return count;
        }

        /// <summary>
        /// Perform a Google query with results starting from the given parameters
        /// </summary>
        /// <param name="query">The query term</param>
        /// <param name="start">The first hit to display (typically 1, 11, 21, ...)</param>
        /// <returns></returns>
        public GoogleResponse makeRequest(string query, int start = 1)
        {
            GoogleResponse result = readFromCache(query, start);

            if (result == null)
            {
                Debug.WriteLine("Starting Google query for " + query);
                var request = new RestRequest(path);
                request.AddParameter("key", key);
                request.AddParameter("cx", cx);
                request.AddParameter("q", query);
                request.AddParameter("start", start);

                result = Execute<GoogleResponse>(request);
                if (result == null || result.items == null)
                {
                    Debug.Print("Unable to perform Google query.");
                    result = null;
                }
                else
                {
                    cache(result, query, start);
                }
            }
            else
            {
                Debug.Print("Query retrieved from cache: " + query);
            }
            return result;
        }

        private T Execute<T>(IRestRequest request) where T : new()
        {
            requestCount++;
            Debug.WriteLine("Google request count: " + requestCount);
            var response = client.Execute<T>(request);
            if (response.ErrorException != null)
            {
                throw new ApplicationException("Error querying Google.", response.ErrorException);
            }
            return response.Data;
        }

        private void cache(GoogleResponse response, string querystring, int start)
        {
            //var id = response.queries.request.First().searchTerms + "###" + response.queries.request.First().startIndex;
            var id = querystring + "###" + start;
            response._id = id;
            mongocollection.Insert(response);
        }

        private GoogleResponse readFromCache(string querystring, int start)
        {
            var id = querystring + "###" + start;
            var query = Query.EQ("_id", id);
            var response = mongocollection.FindOneAs<GoogleResponse>(query);
            return response;
        }
    }

}
