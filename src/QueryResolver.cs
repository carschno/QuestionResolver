using MongoDB.Bson;
using QueryParser;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.IO;
using System.Text.RegularExpressions;

namespace InfoboxParser
{
    public class QueryResolver
    {
        private static Regex urlRegex = new Regex(@"^(/\w://)?[A-Za-y0-9\-]+(\.\w+)+$", RegexOptions.IgnoreCase);
        private static int googleRequests = 0;
        private static bool random_query_selection = true;
        private static int max_test_queries = 10;

        static void Main(string[] args)
        {
            var parser = new QuestionParser(QuestionParser.searchEngine.Google);

            /*************** command line argument substitute ***************/
            if (args.Length == 0)
            {
                args = new string[] { "How old is Michael Phelps?" };
                //args = new string[] { @"C:\Users\faroo\Downloads\AOL-user-ct-collection\user-ct-test-collection-01.txt\user-ct-test-collection-01.txt" };
            }
            /*************** command line argument substitute ***************/


            if (args.Length > 0)
            {
                if (File.Exists(args[0]))
                {
                    // read queries from file
                    foreach (var query in parseQueryFile(args[0], max_test_queries, random_query_selection))
                    { parseQuery(query, parser); }
                }
                else // command line argument is query
                { parseQuery(args[0], parser); }
            }
            else
            {
                Console.WriteLine("Please specify a query (file).");
                Console.ReadKey();
                return;
            }
        }

        /// <summary>
        /// Segment the given query into expressions and identify the query type.
        /// </summary>
        /// <param name="query">A query string</param>
        /// <param name="parser">A QuestionParser object to be used for parsing</param>
        private static void parseQuery(string query, QuestionParser parser = null)
        {
            IEnumerable<string> expressions;
            IEnumerable<string> questionmarkers;
            IEnumerable<string> specifiers;

            if (parser == null)
            { parser = new QuestionParser(QuestionParser.searchEngine.Google); }

            if (isURL(query))
            {
                // forward to url
                Console.WriteLine("{0} is a URL.", query);
                Console.ReadKey();
                return;
            }

            expressions = parser.findExpressions(query, collectionname: "instances", useBoundaryMarkers: true);
            questionmarkers = parser.findQuestionMarkers(query);
            specifiers = parser.findTypes(query).Except(questionmarkers).Except(expressions);
            //specifiers = parser.findSpecifiers(query);

            Debug.Print("Expressions extracted: {0}", String.Join(",", expressions));
            Debug.Print("Question markers extracted: {0}", String.Join(",", questionmarkers));
            Debug.Print("Question specifiers extracted: {0}", String.Join(",", specifiers));

            Console.WriteLine("--------------- Query: '{0}' -------------------", query);
            Console.WriteLine("Partial expressions extracted: " + String.Join(", ", expressions));
            Console.WriteLine();

            if (expressions.Count() > 0)
            { singleView(query, expressions, specifiers); }
            else if (specifiers.Count() > 0)
            { listView(query); return; }
            else
            { notParsable(query); return; }
        }

        /// <summary>
        /// Interpret the query to require a single answer; try to find and print it.
        /// </summary>
        /// <param name="query">The original query.</param>
        /// <param name="expressions">The expressions extracted from the query.</param>
        /// <param name="specifiers">Expressions extracted from the query that specify the wanted facts.</param>
        private static void singleView(string query, IEnumerable<string> expressions, IEnumerable<string> specifiers)
        {
            Debug.Print("Single View.");
            string type;
            var term = expressions.First();
            var instance = findInstance(term);
            if (!(instance.ContainsKey("infoboxtype") || instance.ContainsKey("type")))
            {
                Debug.Print("Changing to List view.");
                listView(query);
                return;
            }

            // find expression type
            if (instance.ContainsKey("infoboxtype"))
            { type = instance["infoboxtype"].ToString(); }
            else if (instance.ContainsKey("type"))
            { type = instance["type"].ToString(); }
            else { return; }
            Debug.Print("Type of '{0}': {1}", term, type);

            // interpreting all expressions except the first (primary) one and all specifiers as properties
            foreach (var expression in expressions.Skip(1))
            {
                bool found = false;
                foreach (var property in instance.Keys)
                {
                    if (InfoboxParser.stem(expression).Equals(InfoboxParser.stem(property)))
                    {
                        Console.WriteLine("{0}'s {1} is: {2}", term, property, instance[property]);
                        found = true;
                    }
                }
                if (!found)
                {
                    Console.WriteLine("Listing sites for {1} and {0}.", term, expression);
                    queryList(term, expression);
                }
            }
            Console.WriteLine("--------------------------------------------------------");
            Console.WriteLine();


            // frequent properties for given (infobox) type:
            var properties = InfoboxParser.frequentProperties(type, top: 10, limit: 50);
            // properties extracted (counts omitted):
            var propertyList = InfoboxParser.keys(properties);

            // FIXME: _id does not exist if result comes from web search
            var formattedName = instance.ContainsKey("_id") ? instance["_id"] : term;
            Console.WriteLine("'{0}' ({2}) is a {1}", term, type, formattedName);

            // output frequent properties for type
            /*Console.WriteLine("Frequent properties for type '{0}':", type);
            foreach (var property in properties)
            { Console.WriteLine("{0}:\t{1}", property.Key, property.Value); }*/

            Console.WriteLine("-------------------------------------------------------");
            Console.WriteLine();

            Console.WriteLine("Properties for {0}:", formattedName);
            foreach (var property in propertyList)
            {
                if (instance.ContainsKey(property))
                { Console.WriteLine("{0}:\t{1}", property, instance[property].ToString()); }
                else
                { Console.WriteLine("{0}:\t{1}", property, "N/A"); }
            }
            Console.ReadKey();
        }

        /// <summary>
        /// Perform a query for the given term and expression and print the resulting page titles.
        /// </summary>
        /// <param name="term"></param>
        /// <param name="expression"></param>
        /// <param name="min"></param>
        private static void queryList(string term, string expression, int min = 5)
        {
            var parser = new QuestionParser(QuestionParser.searchEngine.Google);
            var titles = parser.searchTitles(min, term, expression);
            Console.WriteLine();
            Console.WriteLine("------ Pages for '{0}' and '{1}' ------", term, expression);
            foreach (var title in titles)
            { Console.WriteLine(title); }
        }

        /// <summary>
        /// Print a list of known things (instances) marked with the given type.
        /// </summary>
        /// <param name="inputtype">The input query.</param>
        private static void listView(string inputtype)
        {
            Debug.Print("List View.");
            List<string> instances = new List<string>();
            var types = InfoboxParser.findTypes(inputtype);

            foreach (var type in types)
            { instances.AddRange(InfoboxParser.findPages(new string[] { "infoboxtype", "type" }, type)); }

            Console.WriteLine("------ Instances found for '{0}' ----------", inputtype);
            Console.WriteLine(String.Join(", ", instances));
            Console.ReadKey();
        }

        /// <summary>
        /// From the given file, read a number of lines either from the beginning or in random order (slower).
        /// The file is expected to start with a header line (ignored) and to contain the query string in
        /// the 2nd field of each row, values separated by TABs.
        /// </summary>
        /// <param name="filename">The file to be read.</param>
        /// <param name="limit">The number of queries to be read and returned.</param>
        /// <param name="random">Select queries from the beginning or randomly?</param>
        /// <returns></returns>
        public static List<string> parseQueryFile(string filename, int limit = 10, bool random = false)
        {
            Debug.Print("Opening file " + filename);
            var queries = new List<string>(limit);
            var stream = new StreamReader(File.OpenRead(filename));
            stream.ReadLine();  // skip header line

            if (random)
            {
                Random rnd = new Random();
                // TODO: make this a lazy list?
                var lines = File.ReadAllLines(filename);
                // shuffle list
                foreach (var line in lines.OrderBy(x => rnd.Next()).Take(limit))
                { queries.Add(line.Split('\t')[1]); }
            }
            else
            {
                for (int i = 0; i < limit; i++)
                {
                    var query = stream.ReadLine();
                    queries.Add(query.Split('\t')[1]);
                }
            }

            stream.Close();
            Debug.Print("File {0} closed.", filename);
            return queries;
        }

        /// <summary>
        /// For the given term, try to find a Wikipedia infobox or retrieve its class information through a web search.
        /// </summary>
        /// <param name="name">The name of a thing/instance.</param>
        /// <returns>A Dictionary containing all known properties, including the object's type (infoboxtype).</returns>
        private static IDictionary<string, object> findInstance(string name)
        {
            var instance = searchWikipedia(name);

            if (instance == null || !instance.ContainsKey("infoboxtype"))
            { instance = searchWeb(name); }
            return instance;
        }

        /// <summary>
        /// Look the given term up in the Wikipedia infoboxes.
        /// </summary>
        /// <param name="name">The name of the object/instance.</param>
        /// <returns>A dictionary containing all the properties extracted from the infobox or null if no infobox found.</returns>
        private static IDictionary<string, object> searchWikipedia(string name)
        {
            Dictionary<string, object> result = null;
            BsonDocument instance = InfoboxParser.findPage(name, findSimilar: false);
            if (instance == null)
            { Debug.Print("Could not find '{0}' in database.", name); }
            else
            { result = instance.ToDictionary(); }
            return result;
        }

        /// <summary>
        /// Perform a web search for the given term, extracting the type of the object from the resulting snippets.
        /// Type candidate terms are validated against types that occur in Wikipedia; if none of the candidates
        /// exists in Wikipedia, all are valid.
        /// </summary>
        /// <param name="name">The term to search</param>
        /// <returns>A Dictionary containing the extracted type in the "infoboxtype" key.</returns>
        private static IDictionary<string, object> searchWeb(string name)
        {
            Debug.Print("Performing Google search to determine type of " + name);
            googleRequests++;
            IDictionary<string, object> result;

            var engine = QuestionParser.searchEngine.Google;
            var questionparser = new QuestionParser(engine);

            // extract type candidates from web search
            var candidates = questionparser.extractCandidateTypesFromSnippets(name);

            // intersect candidates with types existing in wikipedia infoboxes
            var intersection = candidates.Intersect(InfoboxParser.getTypes());
            var finalCandidates = intersection.Count() == 0 ? candidates : intersection;

            var counts = new List<KeyValuePair<string, int>>(finalCandidates.Count());
            foreach (var candidate in finalCandidates)
            {
                var count = questionparser.snippetCount(name, candidate);
                //var count = questionparser.urlCount(name, candidate);
                counts.Add(new KeyValuePair<string, int>(candidate, count));
            }

            counts.Sort((candidate1, candidate2) =>
                { return candidate1.Value.CompareTo(candidate2.Value); }
            );

            result = new Dictionary<string, object>(2);
            result.Add("name", name);
            if (counts.Count > 0)
            { result.Add("infoboxtype", counts.Last().Key); }
            return result;
        }

        /// <summary>
        /// Check whether the given string could be a URL.
        /// The check is currently very lax, allowing non-existant URLs and URLs lacking an element.
        /// </summary>
        /// <param name="text">A text</param>
        /// <returns>True if it could be a URL.</returns>
        public static bool isURL(string text)
        {
            return InfoboxParser.pageExists(text) == null ? urlRegex.Match(text).Success : false;
        }

        /// <summary>
        /// This method is called when a query could not be handled correctly by any other method.
        /// </summary>
        /// <param name="query"></param>
        private static void notParsable(string query)
        {
            Console.WriteLine("No information found for query '{0}', forwarding to web search", query);
            Console.ReadKey();
        }
    }
}
