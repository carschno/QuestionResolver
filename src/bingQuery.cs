using Bing;
using System;
using System.Collections.Generic;
using System.Net;
using System.Diagnostics;

namespace QueryParser
{
    class BingQuery
    {
        const string key = @"";
        const string uri = @"https://api.datamarket.azure.com/Bing/Search";
        Dictionary<string, IEnumerable<WebResult>> webCache;
        Dictionary<string, long> countCache;

        public BingQuery()
        {
            webCache = new Dictionary<string, IEnumerable<WebResult>>();
            countCache = new Dictionary<string, long>();
        }

        public IEnumerable<ExpandableSearchResult> query(string query)
        {
            Debug.Print("Perform Bing composite query: " + query);
            var bingContainer = new BingSearchContainer(new Uri(uri));
            bingContainer.Credentials = new NetworkCredential(key, key);
            //var webQuery = bingContainer.Web(query, null, null, null, null, null, null, null);
            var request = bingContainer.Composite("web", query, null, null, null, null, null, null, null, null, null, null, null, null, null);
            return request.Execute();
        }

        public long count(string query)
        {
            long count = 0;
            if (countCache.ContainsKey(query))
            {
                Debug.WriteLine("Fetching count from Bing count cache for " + query);
                count = countCache[query];
            }
            else
            {
                Debug.WriteLine("Performing Bing query to get count for " + query);
                var response = this.query(query);
                foreach (var result in response)
                {
                    if (result.WebTotal.HasValue)
                    {
                        count = result.WebTotal.Value;
                        break;
                    }
                }
            }
            return count;
        }

        public IEnumerable<WebResult> webQuery(string query)
        {
            IEnumerable<WebResult> response;
            if (webCache.ContainsKey(query))
            {
                Debug.WriteLine("Retrieving Bing web query from cache: " + query);
                response = webCache[query];
            }
            else
            {
                Debug.Print("Perform Bing web query: " + query);
                var bingContainer = new BingSearchContainer(new Uri(uri));
                bingContainer.Credentials = new NetworkCredential(key, key);
                var webQuery = bingContainer.Web(query, null, null, null, null, null, null, null);
                webQuery.AddQueryOption("format", "json");
                response = webQuery.Execute();
                webCache.Add(query, response);
            }
            return response;
        }
    }
}
