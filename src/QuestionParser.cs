using GoogleRequest;
using Iveonik.Stemmers;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using MongoDB.Driver.Builders;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;
using InfoboxParser;

namespace QueryParser
{
    public class QuestionParser
    {
        // stemmed categories; TODO: store/retrieve externally (database,file)
        //public static string[] categories = { "citi", "compani", "countri", "lake", "book", "smartphone", "phone", "movie", "film", "medicine", "museum", "gas", "univers", "event", "state", "brand", "tablet", "institut" };
        //public static string[] categories = { "compani", "smartphone", "phone", "brand", "tablet" };
        public static string[] categories;

        static Regex html = new Regex("<([^>])+>");
        private string[] stopwordlists = new string[] { @"common-english-words.txt", @"stopwords.txt", @"mysql-stopwords.txt" };
        private static string[] punctuations = new string[] { "\"", ",", ".", "!", "?", ";", ":", "&", @"\r\n", @"\r", @"\n", "-", "(", ")" };   // "-"
        private static string[] questionMarkers = { "what", "who", "when", "where", "how" };
        private static string[] specifierList = { "relation", "difference", "manufacturer", "price", "size", "color", "colour", "list" };
        //private string stemsFile = @"C:\Users\faroo\Documents\Visual Studio 2012\Projects\QueryParser\stems.txt";
        private HashSet<string> stopwords;
        private GoogleQuery googlequery;
        private BingQuery bingquery;
        private searchEngine engine { get; set; }
        private static string dbname = "cache";
        private static string collectionname = "urls";
        private MongoCollection collection;

        //public Stems stems;

        public enum searchEngine { Google, Bing };

        public QuestionParser(searchEngine engine)
        {
            stopwords = new HashSet<string>();

            //stems = new Stems();
            //stems.readFromFile(stemsFile);

            /// populate stopword list
            foreach (string file in stopwordlists)
            {
                stopwords.UnionWith(File.ReadAllText(file).Split(','));
            }

            collection = new MongoClient().GetServer().GetDatabase(dbname).GetCollection<BsonDocument>(collectionname);

            /// read categories/types from file and stem
            /*            categories = File.ReadAllText(categoriesFile).Split(',');
                        for (int i = 0; i < categories.Count(); i++)
                        {
                            categories[i] = stems.stem(categories[i].Trim());
                        }
                        */

            /// initialize query api engine
            this.engine = engine;
            switch (engine)
            {
                case searchEngine.Google:
                    Debug.WriteLine("QuestionParser using Google.");
                    googlequery = new GoogleQuery();
                    break;
                default:
                    Debug.WriteLine("QuestionParser search engine: " + engine);
                    bingquery = new BingQuery();
                    break;
            }
        }

        /// <summary>
        /// Destructor: save the stems to file
        /// </summary>
        /*~QuestionParser()
        {
            stems.saveToFile(stemsFile);
        }
        */

        /// <summary>
        /// Tokenize the given text and find expressions within.
        /// </summary>
        /// <param name="text"></param>
        /// <returns></returns>
        public IEnumerable<String> findExpressions(String text, string dbname = "cache", string collectionname = "instances", bool useBoundaryMarkers = true)
        {
            Debug.Print("Searching expressions in '{0}' in database {1}, collection {2}", text, dbname, collectionname);
            //var infobox = InfoboxParser.InfoboxParser.findPage(text, findSimilar: false);
            var pagename = InfoboxParser.InfoboxParser.pageExists(text, dbname, collectionname);

            return pagename == null ? findExpressions(tokenize(text), dbname, collectionname, useBoundaryMarkers) : new string[] { pagename };
        }

        /// <summary>
        /// Find expressions in a tokenized text, exploiting stopwords and punctuations as markers.
        /// </summary>
        /// <param name="tokens">An enumeration of tokens</param>
        /// <param name="dbname">The database name</param>
        /// <param name="collectionname">The collection name</param>
        /// <param name="useBoundaryMarkers">Extract expressions heuristically using expression boundary markers such as stop words and punctuation?</param>
        /// <returns>An enumeration of expressions extracted from the text</returns>        
        public IEnumerable<string> findExpressions(IEnumerable<string> tokens, string dbname = "cache", string collectionname = "instances", bool useBoundaryMarkers = true)
        {
            var expressions = new HashSet<String>();
            var currentPhrase = new StringBuilder();
            var matchedTokens = new Boolean[tokens.Count()]; // tokens that have been matches
            for (var i = 0; i < matchedTokens.Length; i++)
            { matchedTokens[i] = false; }

            if (collectionname != null)
            {
                /* Find (partial) expressions matching Wikipedia page titles */
                for (int windowlength = tokens.Count(); windowlength > 0; windowlength--)
                {
                    for (int i = 0; i < tokens.Count() && i + windowlength <= tokens.Count(); i++)
                    {
                        var extract = true;
                        
                        // any of the tokens in window already matches?
                        for (int j = i; j < i + windowlength; j++)
                        { if (matchedTokens[j]) { extract = false; } }

                        // ignore single tokens being stop words/punctuations
                        if (windowlength == 1 && (punctuations.Contains(tokens.ElementAt(i)) || stopwords.Contains(tokens.ElementAt(i))))
                        { extract = false; }

                        if (extract)
                        {
                            var window = tokens.Skip(i).Take(windowlength);
                            var phrase = String.Join(" ", window).Replace(" ,", ",").Replace(" .", ".").Replace(" !", "!").Replace(" ?", "?").Replace(" (", "(").Replace(" )", ")").Trim();
                            Debug.Print("Expression search window: " + phrase);
                            var pagename = InfoboxParser.InfoboxParser.pageExists(phrase, dbname, collectionname);
                            if (pagename != null)
                            {
                                Debug.Print("Found Wikipedia page: " + pagename);
                                expressions.Add(pagename);
                                // mark tokens as matched
                                for (var j = i; j < i + windowlength; j++)
                                { matchedTokens[j] = true; }
                                i += windowlength - 1;
                            }
                        }
                    }
                }

                for (int i = 0; i < tokens.Count(); i++)
                {
                    if (matchedTokens[i])
                    { tokens.ToList().RemoveAt(i); }
                }
            }

            if (useBoundaryMarkers)
            {
                /* Extract expressions based on boundary markers */
                // TODO: limit maximum expression length?
                for (var i = 0; i < tokens.Count(); i++)
                {
                    var token = tokens.ToList()[i];
                    if (matchedTokens[i] || punctuations.Contains(token) || stopwords.Contains(token.ToLower()) || questionMarkers.Contains(token.ToLower()) || specifierList.Contains(token.ToLower()))
                    {
                        if (currentPhrase.ToString().Trim().Length > 0)
                        {
                            Debug.WriteLine("Expression extracted by boundary: " + currentPhrase.ToString().Trim());
                            expressions.Add(currentPhrase.ToString().Trim());
                        }
                        currentPhrase.Clear();
                    }
                    else  // append token to current phrase
                    {
                        currentPhrase.Append(token.Trim() + " ");
                    }
                }
            }

            if (currentPhrase.ToString().Trim().Length > 0)
            {
                Debug.WriteLine("Expression extracted: " + currentPhrase.ToString().Trim());
                expressions.Add(currentPhrase.ToString().Trim());
            }
            return expressions;
        }

        /// <summary>
        /// Finds last token in string that is neither stopword nor punctuation.
        /// Returns null if none found.
        /// </summary>
        /// <param name="snippet">A text passage.</param>
        /// <returns>A string representing a token or null</returns>
        private string findLastToken(string snippet)
        {
            Debug.WriteLine("Searching last token in " + snippet);
            string expr = null;
            var tokens = tokenize(snippet).ToList();
            tokens.Reverse();
            foreach (var token in tokens)
            {
                if (token.Trim().Length > 0 && !punctuations.Contains(token) && !stopwords.Contains(token))
                {
                    expr = token.Trim();   // valid token found
                    Debug.WriteLine("Last token found: " + expr);
                    break;
                }
                else
                {
                    Debug.WriteLine("Ignoring token " + token);
                }
            }
            return expr;    // no valid token found
        }

        /// <summary>
        /// Tokenize text and return a list of question markers
        /// </summary>
        /// <param name="text"></param>
        /// <returns></returns>
        public List<string> findQuestionMarkers(string text)
        {
            var markers = new List<string>();
            foreach (var token in tokenize(text))
            {
                if (questionMarkers.Contains(token.ToLower()))
                {
                    markers.Add(token);
                }
            }
            return markers;
        }

        /// <summary>
        /// Tokenize text and return a list of question specifiers
        /// </summary>
        /// <param name="text"></param>
        /// <returns></returns>
        public List<string> findSpecifiers(string text)
        {
            var specifiers = new List<string>();
            foreach (var token in tokenize(text))
            {
                if (specifierList.Contains(token.ToLower()))
                {
                    specifiers.Add(token);
                }
            }
            return specifiers;
        }

        /// <summary>
        /// Finds all the types in the database matching the given term.
        /// </summary>
        /// <param name="term">The term to match</param>
        /// <param name="dbname">The database to use</param>
        /// <param name="collectionname">The collection to use in the database</param>
        /// <returns></returns>
        public IEnumerable<string> findTypes(string term, string dbname = "cache", string collectionname = "types")
        {
            var specifiers = findExpressions(term, collectionname: collectionname, useBoundaryMarkers: false);
            return specifiers;
        }

        /// <summary>
        /// Retrieve snippets for a query using the search engine set in engine property.
        /// </summary>
        /// <param name="query"></param>
        /// <returns></returns>
        private List<string> getSnippets(String[] queries, int pages = 1)
        {
            switch (engine)
            {
                case QuestionParser.searchEngine.Google:
                    var query = GoogleORQuery(queries);
                    Debug.WriteLine("Retrieving snippets for " + query);
                    var responses = getGoogleResponse(query, pages);
                    return getGoogleSnippets(responses);
                case QuestionParser.searchEngine.Bing:
                    return getBingSnippets(GoogleORQuery(queries));
                default:
                    Debug.WriteLine("No valid search eninge specified for snippet retrieval.");
                    return null;
            }
        }

        /// <summary>
        /// Perform a Google query for the given query
        /// </summary>
        /// <param name="query">A query that will be fed to the search API</param>
        /// <param name="pages">The number of result pages to retrieve</param>
        /// <returns>An array of GoogleResponse objects holding the result page(s)</returns>
        private GoogleResponse[] getGoogleResponse(string query, int pages)
        {
            GoogleResponse[] responses = new GoogleResponse[pages];
            Debug.WriteLine("Retrieving " + pages + " Google response page(s) for " + query);

            for (int i = 0; i < pages; i++)
            {
                var start = i * 10 + 1;
                var response = googlequery.makeRequest(query, start);

                if (response != null)
                { responses[i] = response; }
                else
                { Debug.Print("Failed to retrieve Google response for query " + query); }
            }
            return responses;
        }

        /// <summary>
        /// Retrieve the first n pages and extract snippets and titles for a query using Google.
        /// </summary>
        /// <param name="query">The query</param>
        /// <param name="pages">The number of pages to retrieve.</param>
        /// <returns></returns>
        private List<string> getGoogleSnippets(GoogleResponse[] responses)
        {
            var snippets = new List<string>(20 * responses.Length);
            Debug.WriteLine("Extracting titles and snippets from Google response(s)");

            foreach (var response in responses)
            {
                if (response != null && response.items != null)
                {
                    foreach (var item in response.items)
                    {
                        snippets.Add(item.snippet);
                        snippets.Add(item.title);
                    }
                }
                else
                {
                    Debug.WriteLine("Google response invalid and/or contains no items.");
                }
            }
            return snippets;
        }


        /// <summary>
        /// From an array of GoogleResponse objects, extract the URLs and convert into Uri objects
        /// </summary>
        /// <param name="responses"></param>
        /// <returns></returns>
        private List<string> getGoogleURLs(GoogleResponse[] responses)
        {
            Debug.WriteLine("Extracting URLs from Google response(s)");
            var urls = new List<string>(responses.Length * 10);
            foreach (var response in responses)
            {
                if (response.items == null)
                { Debug.WriteLine("No results found in Google response."); }
                else
                {
                    foreach (var item in response.items)
                    {
                        urls.Add(item.link);
                    }
                }
            }
            return urls;
        }

        /// <summary>
        /// Construct one query joined by OR-operators out of multiple terms
        /// </summary>
        /// <param name="queries"></param>
        /// <returns></returns>
        private string GoogleORQuery(string[] queries)
        {
            var query = new StringBuilder();
            foreach (var q in queries)
            {
                query.Append("\"" + q.Replace("\"", "") + "\" OR ");
                //query.Append(q + " OR ");
            }
            query.Remove(query.Length - 4, 4);
            return query.ToString();
        }

        /// <summary>
        /// Fetch the URLs provided by the search engine for one or multiple terms.
        /// </summary>
        /// <param name="queries">Query terms that will be joined with OR</param>
        /// <param name="pages">The number of pages to retrieve URLs from</param>
        /// <returns>A list of Uri objects</returns>
        private List<string> getURLs(string[] queries, int pages)
        {
            List<string> urls;

            switch (engine)
            {
                case searchEngine.Google:
                    var responses = getGoogleResponse(GoogleORQuery(queries), 1);
                    urls = getGoogleURLs(responses);
                    break;
                default:
                    Debug.WriteLine("No implementation in getURLs for given search engine.");
                    urls = new List<string>();
                    break;
            }
            return urls;
        }

        /// <summary>
        /// Retrieve the data contained in the given URL as a string after purging HTML tags.
        /// </summary>
        /// <param name="url"></param>
        /// <returns>A string with HTML tags removed.</returns>
        private string fetchURL(string url)
        {
            var cleaned = readFromCache(url);

            if (cleaned == null)
            {
                Debug.WriteLine("Fetching URL: " + url);
                var client = new WebClient();
                try
                {
                    var stream = client.OpenRead(url);
                    var content = new StreamReader(stream).ReadToEnd();
                    stream.Close();
                    cleaned = cleanHTML(content);
                    cache(url, cleaned);
                }
                catch (WebException ex)
                {
                    Debug.WriteLine("Could not fetch URL: " + url);
                    Debug.Print(ex.Message);
                    cleaned = "";
                }
            }
            else
            { Debug.Print("Content for URL '{0}' read from cache", url); }

            return cleaned;
        }

        /// <summary>
        /// Retrieve a URL's content from the database.
        /// </summary>
        /// <param name="url"></param>
        /// <returns></returns>
        private string readFromCache(string url)
        {
            string content;

            // query for "url" is provisional (should be the id)
            var query = Query.Or(Query.EQ("_id", url.Replace('.', '_')), Query.EQ("url", url.Replace('.', '_')));
            var doc = collection.FindOneAs<BsonDocument>(query);

            if (doc == null)
            {
                Debug.Print("URL {0} not found in cache.", url);
                content = null;
            }
            else
            {
                Debug.Print("URL {0} retrieved from cache.", url);
                content = doc.GetValue("content").AsString;
            }

            return content;
        }

        /// <summary>
        /// Store a URL's content in database.
        /// </summary>
        /// <param name="url"></param>
        /// <param name="content"></param>
        private void cache(string url, string content)
        {
            var doc = new BsonDocument();
            doc.Add("_id", new BsonString(url.Replace('.', '_')));
            doc.Add("content", BsonValue.Create(content));
            doc.Add("fetched", new BsonDateTime(DateTime.Now));

            var result = collection.Insert(doc);
            if (result.Ok)
            { Debug.Print("URL {0} stored in cache.", url); }
            else
            {
                Debug.Print("Error storing URL {0} in cache:", url);
                Debug.Print(result.LastErrorMessage);
            }
        }

        /// <summary>
        /// Remove all HTML tags from the given string.
        /// </summary>
        /// <param name="text"></param>
        /// <returns></returns>
        private string cleanHTML(string text)
        {
            text = html.Replace(text, "");
            text = text.Replace("\r\n", Environment.NewLine).
                Replace("\r", Environment.NewLine).
                Replace("\n", Environment.NewLine).
                Replace('\t', ' ');
            return text.Trim();
        }

        /// <summary>
        /// Retrieve snippets for a query using Bing.
        /// </summary>
        /// <param name="query"></param>
        /// <returns></returns>
        private List<string> getBingSnippets(String query)
        {
            Debug.WriteLine("Retrieving snippets with Bing for " + query);
            var response = bingquery.webQuery(query);
            var snippets = new List<string>();

            foreach (var result in response)
            {
                snippets.Add(result.Description);
            }
            return snippets;
        }


        /// <summary>
        /// Extract type candidates for the given search term from a text passage, using (pre-defined) lexico-syntactic patterns.
        /// </summary>
        /// <param name="snippet"></param>
        /// <param name="searchTerm"></param>
        /// <returns></returns>
        private HashSet<string> patternSearch(string snippet, string searchTerm)
        {
            Debug.WriteLine("Searching for lexico-syntactic pattern in: " + snippet);
            var candidates = new HashSet<string>();
            var tokens = tokenize(snippet);
            // database collection holding valid types:
            string dbcollection = null;

            // use last token in search term only for matching:
            var searchTokens = tokenize(searchTerm);

            for (int i = 0; i < tokens.Length; i++)
            {
                var token = tokens[i];
                switch (token)
                {
                    case ("such"):
                        Debug.WriteLine("Found keyword 'such' in snippet: " + snippet);
                        if (i > 0 && i + 3 < tokens.Length)
                        {
                            Debug.WriteLine(tokens[i - 1] + " " + token + " " + tokens[i + 1] + " " + tokens[i + 2] + " " + tokens[i + 3]);
                            if (tokens[i + 1].Equals("as") && tokens[i + 2].Equals(searchTokens.First()))
                            {
                                // "* such as <searchTerm>" found
                                // TODO: allow for "such as (the/a/an) <searchTerm>
                                Debug.WriteLine("Pattern '* such as <searchTerm>' detected.");
                                if (tokens.Length >= i + 2 || punctuations.Contains(tokens[i + 3]) || stopwords.Contains(tokens[i + 3]))
                                {
                                    // candidate found
                                    var expressions = findExpressions(tokens.Take(i), collectionname: dbcollection);
                                    if (expressions.Count() > 0)
                                    {
                                        var candidate = tokenize(expressions.Last()).Last();
                                        if (!candidate.ToLower().Equals(searchTerm.ToLower()))
                                        // { candidates.Add(stems.stem(candidate)); }
                                        { candidates.Add(candidate); }
                                    }
                                }
                            }
                        }
                        else if (i + 3 < tokens.Length)
                        {
                            if (tokens[i + 2].Equals("as") && tokens[i + 3].Equals(searchTokens.First()))
                            {
                                Debug.WriteLine("Pattern 'such * as <searchTerm>' detected.");
                                if (tokens.Length >= i + 4 || punctuations.Contains(tokens[i + 4]) || punctuations.Contains(tokens[i + 4]))
                                {
                                    var candidate = tokens[i + 1];
                                    Debug.WriteLine("Candidate found: " + candidate);
                                    if (!candidate.ToLower().Equals(searchTerm.ToLower()) && !punctuations.Contains(candidate) && !stopwords.Contains(candidate))
                                    // { candidates.Add(stems.stem(candidate)); }
                                    { candidates.Add(candidate); }
                                }
                            }
                        }
                        break;

                    case ("other"):
                        Debug.WriteLine("Found keyword 'other' in snippet: " + snippet);
                        if (i >= 2 && tokens.Length > i + 1)
                        {
                            if (tokens[i - 2].ToLower().Equals(searchTokens.Last().ToLower()) && (tokens[i - 1].ToLower().Equals("and") || tokens[i - 1].ToLower().Equals("or")))
                            {
                                Debug.WriteLine("Pattern '<searchTerm> and/or other' found.");
                                var expressions = findExpressions(tokens.Skip(i + 1), collectionname: dbcollection);
                                /// extract last token from first expression extracted:
                                if (expressions.Count() > 0)
                                {
                                    var candidate = tokenize(expressions.First()).Last();
                                    if (!candidate.ToLower().Equals(searchTerm.ToLower()))
                                    //{ candidates.Add(stems.stem(candidate)); }
                                    { candidates.Add(candidate); }
                                }
                            }
                        }
                        break;

                    case ("including"):
                    case ("especially"):
                        Debug.WriteLine("Found keyword 'including' or 'especially' in snippet: " + snippet);
                        if (i >= 1 && tokens.Length > i)
                        {
                            if (tokens[i + 1].ToLower().Equals(searchTokens.First()))
                            {
                                // extract expressions occurring in the the pattern
                                var expressions = findExpressions(tokens.Take(i), collectionname: dbcollection);
                                // take last expressions before keyword and its last token as a type candidate
                                if (expressions.Count() > 0)
                                {
                                    var candidate = tokenize(expressions.Last()).Last();
                                    if (!candidate.ToLower().Equals(searchTerm.ToLower()))
                                    //{ candidates.Add(stems.stem(candidate)); }
                                    { candidates.Add(candidate); }
                                }
                            }
                        }
                        break;
                }
            }
            return candidates;
        }

        /// <summary>
        /// Search for the given regular expressions in the snippet and return stems of matches.
        /// This relies on the Regex having one (or more) groups defined, the 1st group in the regex is extracted.
        /// </summary>
        /// <param name="snippet">The text to search in.</param>
        /// <param name="regex">The regular expression (should contain one group definition)</param>
        /// <returns></returns>
        private HashSet<string> patternSearch(string snippet, Regex[] regexs)
        {
            var candidates = new HashSet<string>();
            while (snippet.Contains("  "))
            {
                snippet = snippet.Replace("  ", " ");
            }
            foreach (Regex regex in regexs)
            {
                Debug.WriteLine("Searching for patterns in " + snippet + " using regular expression " + regex.ToString());
                var matches = regex.Matches(snippet);
                foreach (Match match in matches)
                {
                    string matchstring = match.Groups[1].Value;
                    /* search expressions in matching string and add last one to type candidates
                    var allTypes = findExpressions(matchstring);
                    if (allTypes.Count > 0)
                    {
                        // only adding last expression to candidates
                        types.Add(allTypes.Last().ToLower());
                        Debug.WriteLine("Type '" + types.Last() + "' extracted from " + snippet);
                    }*/
                    var candidate = findLastToken(matchstring);
                    if (candidate != null && candidate.Length > 0)
                    {
                        /*var stem = stems.stem(candidate);
                        candidates.Add(stem);*/
                        candidates.Add(candidate);
                        Debug.WriteLine("Candidate type found: " + candidate);
                    }
                }
            }
            return candidates;
        }

        /// <summary>
        /// Generate a list of queries to use for type/category search of the given term and
        /// regular expressions that are used to extract text within the returned snippets.
        /// </summary>
        /// <param name="term"></param>
        /// <returns></returns>
        private string[] getQueries(string term)
        {
            string[] queries;
            switch (engine)
            {
                case QuestionParser.searchEngine.Google:
                    queries = new string[] {
                        "\"* such as " + term + "\"",
                        "\"such * as " + term + "\"",
                        "\"" + term + " or other *\"",
                        "\"" + term + " and other *\"",
                        "\"* including " + term + "\"",
                        "\"* especially " + term + "\""
                    };
                    break;
                default:
                    queries = new string[] { term };
                    break;
            }
            return queries;
        }

        /// <summary>
        /// Return a list of regular expressions searching statically defined lexico-syntactic patterns.
        /// </summary>
        /// <param name="term"></param>
        /// <returns></returns>
        private static Regex[] getRegex()
        {
            var regex = new Regex[] {
                new Regex(@"(.+)\s+such\s+as\s+", RegexOptions.IgnoreCase),
                new Regex(@"such\s+(.+)\s+as\s+", RegexOptions.IgnoreCase),
                new Regex(@"\s+or\s+other\s+(.+)+", RegexOptions.IgnoreCase),
                new Regex(@"\s+and\s+other\s+(.+)+", RegexOptions.IgnoreCase),
                new Regex(@"(.+)\s+including\s+", RegexOptions.IgnoreCase),
                new Regex(@"(.+)\s+especially\s+", RegexOptions.IgnoreCase)
            };
            return regex;
        }

        public static string[] tokenize(string text)
        {
            foreach (var punct in punctuations)
            {
                // insert white spaces around punctuations
                //text = text.ToLower().Replace(punct + " ", " " + punct + " ").Replace(" " + punct, " " + punct + " ");
                text = text.ToLower().Replace(punct, " " + punct + " ");
            }
            while (text.Contains("  "))
            { text = text.Replace("  ", " "); }
            var tokens = text.Split(' ');
            return tokens;
        }

        /// <summary>
        /// For a given term, retrieve snippets according to patterns defined in patternSearch()
        /// and find expressions surrounding it in the snippets.
        /// 
        /// </summary>
        /// <param name="term"></param>
        /// <returns></returns>
        public IEnumerable<string> extractCandidateTypesFromSnippets(string term)
        {
            var candidates = new HashSet<string>();
            var regex = getRegex();
            var snippets = getSnippets(getQueries(term.ToLower()));

            foreach (var snippet in snippets)
            {
                foreach (var candidate in patternSearch(snippet, term.ToLower()))
                {
                    Debug.WriteLine("Found type candidate: " + candidate);
                    candidates.Add(candidate);
                }
            }
            return candidates;
        }

        /// <summary>
        /// Calculate the Jaccard coefficient for the given terms.
        /// </summary>
        /// <param name="term1"></param>
        /// <param name="term2"></param>
        /// <returns></returns>
        public float jaccard(string term1, string term2)
        {
            //var stemQuery = this.stemQuery(term2);
            Debug.WriteLine("Getting Jaccard coefficient for " + term1 + " and " + term2);
            var countOR = getCount(term1 + " OR " + term2);
            var countAND = getCount(term1 + " AND " + term2);
            return jaccard(countOR, countAND);
        }

        /// <summary>
        /// Compute the Jaccard coeeficient for two numbers. The numbers represent occurrences of two search terms in the web, referring to pages
        /// containing one of two terms (countOR) or both terms (countAND). The result should always be between 0 and 1. Note that the counts
        /// as returned from web search engines are usually estimates and therefore might be wrong leading to unrealistic results.
        /// </summary>
        /// <param name="countOR">The number of pages containing either term.</param>
        /// <param name="countAND">The number of pages containing both terms.</param>
        /// <returns>The Jaccard coefficient.</returns>
        private float jaccard(float countOR, float countAND)
        {
            return countAND / (countOR - countAND);
        }

        /// <summary>
        /// Calculate the Sorensen coefficient for the given terms.
        /// </summary>
        /// <param name="term1"></param>
        /// <param name="term2"></param>
        /// <returns></returns>
        public float sorensen(string term1, string term2)
        {
            //var stemQuery = this.stemQuery(term2);
            Debug.WriteLine("Getting Sorensen coefficient for " + term1 + " and " + term2);
            var countOR = getCount(term1 + " OR " + term2);
            var countAND = getCount(term1 + " AND " + term2);
            return sorensen(countOR, countAND);
        }

        /// <summary>
        /// Compute the Sorensen coeeficient for two numbers. The numbers represent occurrences of two search terms in the web, referring to pages
        /// containing one of two terms (countOR) or both terms (countAND). The result should always be between 0 and 1. Note that the counts
        /// as returned from web search engines are usually estimates and therefore might be wrong leading to unrealistic results.
        /// </summary>
        /// <param name="countOR">The number of pages containing either term.</param>
        /// <param name="countAND">The number of pages containing both terms.</param>
        /// <returns>The Sorensen coefficient.</returns>
        private float sorensen(float countOR, float countAND)
        {
            return 2 * countAND / countOR;
        }

        /// <summary>
        /// Construct an OR-query from all words that have the same stem as the given term.
        /// </summary>
        /// <param name="word"></param>
        /// <returns>A query is in the form "(term1 OR term2 OR ...)"</returns>
        /*private string stemQuery(string word)
        {
            var query = new StringBuilder();
            query.Append("(");
            foreach (var w in stems.getAllWords(word))
            {
                query.Append(w + " OR ");
            }
            query.Length -= 4;
            query.Append(")");
            return query.ToString();
        }*/

        /// <summary>
        /// Perform a query for occurrences of both terms close to each other.
        /// </summary>
        /// <param name="term1"></param>
        /// <param name="term2"></param>
        /// <returns></returns>
        public long nearCount(string term1, string term2)
        {
            string query;
            switch (engine)
            {
                case searchEngine.Google:
                    query = term1 + " - " + term2;
                    break;
                default:
                    throw new NotImplementedException();
            }
            return getCount(query);
        }

        /// <summary>
        /// Retrieve the number of matches for the given query string.
        /// </summary>
        /// <param name="query">The query to execute.</param>
        /// <returns>The (estimated) number of hits returned from the web search eninge.</returns>
        private long getCount(string query)
        {
            long count;

            switch (engine)
            {
                case searchEngine.Google:
                    count = googlequery.getCount(query);
                    break;
                default:
                    throw new NotImplementedException();
            }
            return count;
        }

        /// <summary>
        /// Perform a NEAR count (count hits for the two terms near to each other) after
        /// expanding the given words to all other known words with the same stem.
        /// </summary>
        /// <param name="word1"></param>
        /// <param name="word2"></param>
        /// <returns></returns>
        /*public long nearStemCount(string word1, string word2)
        {
            return nearCount(stemQuery(word1), stemQuery(word2));
        }*/

        /// <summary>
        /// Retrieve snippets (and titles) for the given term and search in results for the candidate.
        /// </summary>
        /// <param name="term">The unmodified term is used to retrieve snippets.</param>
        /// <param name="candidate">The candidate is interpreted as a stem and expanded to all words yielding that stem</param>
        /// <returns>The total count of any of the de-stemmed words in any of the snippets</returns>
        public int snippetCount(string term, string candidate)
        {
            var snippets = getSnippets(new string[] { term });
            return occurrenceCount(snippets, candidate);
        }

        /// <summary>
        /// Get the number of times, the candidate occurs in the documents matching the given term.
        /// </summary>
        /// <param name="term">The term to search for finding documents</param>
        /// <param name="candidate">The second term to count occurrences of in the retrieved documents</param>
        /// <returns>The number of times the candidate terms occurs in the documents</returns>
        public int urlCount(string term, string candidate)
        {
            int count = 0;
            foreach (var url in getURLs(new string[] { term }, 1))
            {
                var text = fetchURL(url);
                if (text != null)
                {
                    count += occurrenceCount(text, candidate);
                }
            }
            return count;
        }

        /// <summary>
        /// Count the occurrences of the words matching the given stem in the given list of text passages.
        /// </summary>
        /// <param name="texts">A list of text passages.</param>
        /// <param name="term">A term to count in all texts.</param>
        /// <returns>The total number of occurrences of the term in all of the texts</returns>
        private int occurrenceCount(List<string> texts, string term)
        {
            int count = 0;
            foreach (var text in texts)
            { count += occurrenceCount(text, term); }
            return count;
        }

        /// <summary>
        /// How often does the term occur in the given text?
        /// </summary>
        /// <param name="text">The base text</param>
        /// <param name="term">The term to count</param>
        /// <returns>The number of occurrences of the term in the base text</returns>
        private int occurrenceCount(string text, string term)
        {
            return (text.Length - text.ToLower().Replace(term.ToLower(), "").Length) / term.Length;
        }

        /// <summary>
        /// Perform a web search for all the given expressions and return the titles of resulting pages.
        /// </summary>
        /// <param name="min">The minimum number of hits to return (determines the number of queries).</param>
        /// <param name="terms">The terms to search for.</param>
        /// <returns>A list of titles.</returns>
        public IEnumerable<string> searchTitles(int min = 10, params string[] terms)
        {
            var titles = new List<string>(min);
            var query = String.Join(" ", terms);
            switch (engine)
            {
                case (searchEngine.Google):
                    while (titles.Count < min)
                    {
                        var response = googlequery.makeRequest(query, titles.Count + 1);
                        if (response != null)
                        {
                            foreach (var item in response.items)
                            { titles.Add(item.title); }
                        }
                        else
                        { Debug.Print("No search results for " + query); }
                    }
                    break;
                default:
                    throw new NotImplementedException();
            }
            return titles;
        }
    }

    /// <summary>
    /// A class (no longer) used to stem terms and store the list of stemms including mappings to the original words.
    /// </summary>
    public class Stems
    {
        Dictionary<string, HashSet<string>> stems;
        IStemmer stemmer;
        private static string dbname = "cache";
        private static string collectionname = "stems";
        private MongoCollection collection;

        public Stems()
        {
            stems = new Dictionary<string, HashSet<string>>();
            stemmer = new EnglishStemmer();
            collection = new MongoClient().GetServer().GetDatabase(dbname).GetCollection(collectionname);
        }

        public string stem(string word)
        {
            var stem = stemmer.Stem(word);
            /// store stem and word
            if (!stems.ContainsKey(stem))
            {
                stems[stem] = new HashSet<string>();
            }
            stems[stem].Add(word);
            return stem;
        }

        /// <summary>
        /// Read stems from given file and add to existing stems
        /// </summary>
        /// <param name="filename"></param>
        public void readFromFile(string filename)
        {
            //while ((line = file.ReadLine()) != null)
            foreach (var line in File.ReadAllLines(filename))
            {
                // line should be <stem><TAB><word1>,<word2>,...
                var entry = line.Split('\t');
                if (entry.Length < 2)
                {
                    Debug.WriteLine("Ignoring invalid line in filename:");
                    Debug.WriteLine(line);
                }
                // add hashset for newly read stem
                if (!stems.ContainsKey(entry[0]))
                {
                    stems[entry[0]] = new HashSet<string>();
                }
                foreach (var word in entry[1].Split(','))
                {
                    stems[entry[0]].Add(word);
                }
            }
        }

        // TODO: use mongo db for stems
        private Dictionary<string, List<string>> readStems()
        {
            var stems = new Dictionary<string, List<string>>();

            foreach (BsonDocument doc in collection.FindAllAs<BsonDocument>())
            {
                var stem = doc["_id"].AsString;
                var words = doc["words"].AsBsonArray;
                var wordlist = BsonSerializer.Deserialize<List<string>>(words.ToJson());
                stems.Add(stem, wordlist);
            }
            return stems;
        }

        /// <summary>
        /// Write stems to existing file (overwriting file!)
        /// </summary>
        /// <param name="filename"></param>
        public void saveToFile(string filename)
        {
            StreamWriter file = new StreamWriter(filename);
            foreach (var stem in stems.Keys)
            {
                // line should be <stem><TAB><word1>,<word2>,...
                StringBuilder line = new StringBuilder();
                line.Append(stem + "\t");
                foreach (var word in stems[stem])
                {
                    line.Append(word + ",");
                }
                line.Length -= 1;
                file.WriteLine(line);
            }
            file.Close();
        }

        /// <summary>
        /// Get all words that have the given stem.
        /// If no word is known to have this stem, only the stem is returned.
        /// </summary>
        /// <param name="stem"></param>
        /// <returns>A HashSet containing all known words with the given stem.</returns>
        public HashSet<string> getAllWords(string stem)
        {
            HashSet<string> words;
            if (stems.ContainsKey(stem))
            {
                words = stems[stem];
            }
            else
            {
                Debug.WriteLine("No words with stem found: " + stem);
                words = new HashSet<string>();
                words.Add(stem);
            }
            return words;
        }
    }
}
