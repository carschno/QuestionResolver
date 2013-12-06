using ICSharpCode.SharpZipLib.BZip2;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Builders;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text.RegularExpressions;
using Iveonik.Stemmers;
using QueryParser;
using System.Text;

namespace InfoboxParser
{
    public class InfoboxParser
    {
        //static const Regex textStart = new Regex(@"<text[^>]*>.*");
        //static const Regex textEnd = new Regex(@"</text>");
        static Regex infoboxStart = new Regex(@"{{\s*Infobox([^\|\}]+)");
        static Regex infoBoxEnd = new Regex("}}");
        static Regex propertyRegex = new Regex(@"\|([^=]+)=([^=\|]+)?");
        static Regex wikiLabeledLink = new Regex(@"\[\[[^\]]+\|([^\|\]]+)\]\]");
        static Regex wikiLink = new Regex(@"\[\[([^\|\]]+)\]\]");
        static Regex externalLink = new Regex(@"\[[^\]]+\]");
        static Regex wikiRef = new Regex(@"\{\{([^\}]+)\}\}");
        static Regex comment = new Regex(@"&lt;!--[^-]+--&gt;");
        static Regex tag = new Regex(@"\&lt;[^;]+\&gt;");
        static Regex pageTitle = new Regex(@"<title>([^<]+)</title>");
        static Regex redirectRegex = new Regex(@"<redirect title\s*=\s*""([^""]+)""\s*/>");
        static Regex birthDateRegex = new Regex(@"{{[Bb]irth date( and age)?\s*\|(.f=[A-Za-z]+\|)?(?<year>\d\d\d\d)\|(?<month>\d\d?)\|(?<day>\d\d?)[^}]*}}");
        static Regex deathDateRegex = new Regex(@"{{[Dd]eath date( and age)?\s*\|(.f=[A-Za-z]+\|)?(?<year>\d\d\d\d)\|(?<month>\d\d?)\|(?<day>\d\d?)[^}]*}}");

        static Regex[] cleaningRegex = { wikiLabeledLink, wikiLink, wikiRef };
        static Regex[] removingRegex = { externalLink, comment, tag };
        static Regex[] dateRegex = { birthDateRegex, deathDateRegex };

        static string[] typefields = { "infoboxtype", "type" };
        //static string[] typefields = { "infoboxtype" };

        /// <summary>
        /// Store the infoboxes found in the given file to a database.
        /// </summary>
        /// <param name="infile">The Wikipedia XML file to extract infoboxes from</param>
        /// <param name="max">Stop after this number of infoboxes (never if 0)</param>
        /// <param name="skip">Skip the first n entries.</param>
        static void storeInfoboxes(string infile, int max = 0, int skip = 0)
        {
            storeInfoboxInstances(infile, limit: max, skip: skip);
            //var filter = new Dictionary<string, string>();
            //filter.Add("type", "city");

            //var infoboxes = findInfoboxes(getReader(infile), filter: null, number: max);

            //Console.WriteLine("Total infoboxes found: {0}", infoboxes.Count);
            //storeInstancesToMongoDB(infoboxes);

            /*if (infoboxes.Count <= 3)
            {
                foreach (var infobox in infoboxes)
                {
                    printInfobox(infobox);
                }
            }
            else
            {
                printProperties(infoboxes);
                printValues(infoboxes);
            }*/
        }

        /// <summary>
        /// Print all properties of the given infobox.
        /// </summary>
        /// <param name="infobox">A dictionary containing the infobox parameteres</param>
        /// <param name="properties">If specified, print only the parameters listed here</param>
        private static void printInfobox(Dictionary<string, string> infobox, List<string> properties = null)
        {
            Console.WriteLine("----------------------------------------------------------------------");
            foreach (var property in infobox.Keys)
            {
                if (properties == null || properties.Contains(property.ToLower()))
                { Console.WriteLine("{0}:\t{1}", property, infobox[property]); }
            }
            Console.WriteLine("----------------------------------------------------------------------");
        }

        /// <summary>
        /// Print all the properties found in any of the given infoboxes, sorted by frequency.
        /// </summary>
        /// <param name="infoboxes">A List of dictionaries representing infoboxes</param>
        private static void printProperties(IList<IDictionary<string, string>> infoboxes)
        {
            var counts = countProperties(infoboxes);
            var properties = new List<string>(counts.Keys);
            //properties.Remove("type");
            properties.Sort((string prop1, string prop2) =>
            {
                return counts[prop1].CompareTo(counts[prop2]);
            });

            Console.WriteLine("-------------- Properties -----------------------");
            foreach (var property in properties)
            {
                Console.WriteLine("{0}:\t{1}", property, Convert.ToString(counts[property]));
            }
        }

        /// <summary>
        /// Print all the distinct values found in the given infoboxes for a given property name.
        /// </summary>
        /// <param name="infoboxes">A list of dictionaries representing infoboxes</param>
        /// <param name="property">The property to search distinct values for</param>
        private static void printValues(IList<IDictionary<string, string>> infoboxes, string property = "type")
        {
            var categories = countValues(infoboxes, property);
            var values = new List<string>(categories.Keys);

            values.Sort((string prop1, string prop2) =>
            {
                return categories[prop1].CompareTo(categories[prop2]);
            });

            Console.WriteLine("-------------- Values for '{0}' -------------------", property);
            foreach (var category in values)
            {
                Console.WriteLine("{0}:\t{1}", category, Convert.ToString(categories[category]));
            }
        }

        /// <summary>
        /// Search for infoboxes in a Wikipedia XML dump file and extract contained properties.
        /// Each infobox is represented as a dictionary where the keys are the properties.
        /// If the filter argument is given, only infoboxes are returned in which the properties defined
        /// as filter keys contain the string defined in the filter (case-insensitive).
        /// </summary>
        /// <param name="filename">The Wikipedia XML dump file to parse</param>
        /// <param name="filter">A Dictionary where the keys and values are applied to search for matching infoboxes</param>
        /// <param name="number">The maximum number of infoboxes to return (0 is infinite)</param>
        /// <returns>A list of infoboxes, each represented as a dictionary.</returns>
        public static IList<IDictionary<string, string>> parseWikipedia(StreamReader reader, Dictionary<string, string> filter = null, int number = 0, int skip = 0)
        {
            Debug.Print("Searching for infoboxes.");
            var infoboxes = new List<IDictionary<string, string>>();
            int nested = 0; // keep track of depth of nested '{{' / '}}' structures
            Dictionary<string, string> currentInfobox = null;
            string line;
            var count = 0;
            var title = "";

            while ((line = reader.ReadLine()) != null && (number > 0 && count < number))
            {
                // count nesting level of '{{ ... }}' structures by counting occurrences of start and end tags
                if (line.Contains("{{"))
                { nested += (line.Length - line.Replace("{{", ".").Length); }
                if (line.Contains("}}"))
                { nested -= (line.Length - line.Replace("}}", ".").Length); }


                //********************* Find Page Title/Redirect *********************/
                var titleMatch = pageTitle.Match(line);
                if (titleMatch.Success)
                {
                    title = titleMatch.Groups[1].Value;
                    Debug.Print("Title found: " + title);
                    continue;
                }
                var redirectMatch = redirectRegex.Match(line);
                if (redirectMatch.Success)
                {
                    count++;
                    var redirect = redirectMatch.Groups[1].Value;
                    Debug.Print("Redirect found: {0} -> {1}", title, redirect);
                    Debug.Assert(title.Length > 0);
                    currentInfobox = new Dictionary<string, string>(2);
                    currentInfobox.Add("pagetitle", title);
                    currentInfobox.Add("redirect", redirect);
                    infoboxes.Add(currentInfobox);
                    currentInfobox = null;
                    title = "";
                    continue;
                }
                //********************* Find Page Title/Redirect *********************/

                /********************* Infobox end **************************/
                if (currentInfobox != null && nested <= 0)
                {
                    Debug.Print("Infobox end detected: " + line);
                    if (filter == null)
                    { infoboxes.Add(currentInfobox); }
                    else
                    {
                        // apply filter
                        var keepCurrent = true;
                        foreach (var filterkey in filter.Keys)
                        {
                            if (!(currentInfobox.ContainsKey(filterkey) && currentInfobox[filterkey].ToLower().Contains(filter[filterkey].ToLower())))
                            {
                                keepCurrent = false;
                                break;
                            }
                        }
                        if (keepCurrent)
                        { infoboxes.Add(currentInfobox); }

                    }
                    currentInfobox = null;
                    title = "";
                    continue;
                }
                /********************* Infobox end **************************/

                /********************* Infobox start ************************/
                // are we inside a page (i.e. title is set)
                if (title.Length > 0)
                {
                    var infoBoxMatch = infoboxStart.Match(line);
                    if (infoBoxMatch.Success)
                    {
                        count++;
                        Debug.Print("Infobox start detected: " + line);
                        nested = 1;
                        if (count > skip)
                        {
                            currentInfobox = new Dictionary<string, string>();
                            // 'type' taken from infobox category
                            if (title.Length > 0)
                            { currentInfobox.Add("pagetitle", title); }
                            currentInfobox.Add("infoboxtype", cleanWiki(infoBoxMatch.Groups[1].Value));
                        }
                    }
                    //********************* Infobox start ************************/

                    /********************* Property extraction ******************/
                    if (currentInfobox != null)
                    {
                        foreach (var property in extractProperties(line))
                        {
                            var prop = property.Key;
                            var val = property.Value;

                            // filter infoboxes
                            if (filter != null && filter.ContainsKey(prop))
                            {
                                if (!val.ToLower().Contains(filter[prop].ToLower()))
                                {
                                    Debug.Print("Omitting infobox with entry {0}: {1}", prop, val);
                                    currentInfobox = null;
                                    break;
                                }
                            }

                            if (currentInfobox.ContainsKey(prop))
                            { Debug.Print("Warning: Double property specification in infobox: {0}", prop); }
                            else
                            { currentInfobox.Add(prop, val); }
                        }
                    }
                }
                //********************* Property extraction ******************/
            }
            return infoboxes;
        }

        /// <summary>
        /// Parse the given Wikipedia XML file and extract and store infoboxes to database in batches of 100.
        /// </summary>
        /// <param name="filename">The file to parse</param>
        /// <param name="limit">Stop after this number of infoboxes has been found.</param>
        /// <param name="skip">Skip the first n infoboxes</param>
        static void storeInfoboxInstances(string filename, int limit = 0, int skip = 0)
        {
            var defaultBatch = 100;
            var batchSize = (limit > 0 && defaultBatch > limit) ? limit : defaultBatch;
            var reader = getReader(filename);
            var count = 0;

            while ((limit == 0 || count < limit) && !reader.EndOfStream)
            {
                var infoboxes = parseWikipedia(reader, number: batchSize, skip: skip);
                if (infoboxes.Count == 0)
                { Debug.Print("No more infoboxes found (total: {0}).", count); }
                else
                {
                    count += storeInstancesToMongoDB(infoboxes);
                }
            }
            reader.Close();
            Console.WriteLine("{0} infoboxes stored in database.", count);
        }

        /// <summary>
        /// Open a StreamReader for the given file.
        /// Detects compressed files (currently GZip and BZip2) by their ending.
        /// </summary>
        /// <param name="filename">The file to open</param>
        /// <returns>An open StreamReader for the given file.</returns>
        private static StreamReader getReader(string filename)
        {
            StreamReader reader;

            if (filename.EndsWith(".gz"))
            {
                Debug.Print("Opening GZip-compressed file {0}.", filename);
                FileStream original = new FileStream(filename, FileMode.Open, FileAccess.Read);
                GZipStream decompressed = new GZipStream(original, CompressionMode.Decompress);
                reader = new StreamReader(decompressed);
            }
            else if (filename.EndsWith(".bz2"))
            {
                Debug.Print("Opening BZip2-compressed file {0}.", filename);
                var stream = new BZip2InputStream(new FileStream(filename, FileMode.Open, FileAccess.Read));
                reader = new StreamReader(stream);
            }
            else
            { reader = new StreamReader(new FileStream(filename, FileMode.Open)); }

            return reader;
        }

        /// <summary>
        /// Separate property and value from a live in the form "PROPERTY = ..."
        /// </summary>
        /// <param name="line">A string, typically a line read from a Wikipedia XML dump</param>
        /// <returns>A Tuple containing the property and the value</returns>
        public static Dictionary<string, string> extractProperties(string line)
        {
            //List<Tuple<string, string>> results = new List<Tuple<string, string>>();
            Debug.Print("Extracting properties from line:");
            Debug.Print(line);

            var results = new Dictionary<string, string>();
            var propMatch = propertyRegex.Match(cleanWiki(line));
            while (propMatch.Success)
            {
                Debug.Print("Extracting properties in infobox: " + line);
                var property = propMatch.Groups[1].Value.Trim().ToLower();
                var value = propMatch.Groups[2].Value.Trim();
                Debug.Print("Property: {0}:\t{1}", property, value);

                if (property.Length > 0 && value.Length > 0 && !results.ContainsKey(property))
                { results.Add(property, value); }
                propMatch = propMatch.NextMatch();
            }
            return results;
        }

        /// <summary>
        /// Count the frequencies for all keys contained in the given list of dictionaries.
        /// </summary>
        /// <param name="infoboxes">A list of dictionaries, typically representing an infobox.</param>
        /// <returns>A dictionary mapping the keys found to their frequencies.</returns>
        private static Dictionary<string, int> countProperties(IEnumerable<IDictionary<string, string>> infoboxes)
        {
            var counts = new Dictionary<string, int>();

            foreach (var infobox in infoboxes)
            {
                foreach (var prop in infobox.Keys)
                {
                    if (counts.ContainsKey(prop))
                    { counts[prop]++; }
                    else
                    { counts.Add(prop, 1); }
                }
            }

            return counts;
        }

        /// <summary>
        /// Count the frequencies of all properties occurring in the given infoboxes.
        /// </summary>
        /// <param name="infoboxes">A List of BsonDocuments representing infoboxes</param>
        /// <returns>A Dictionary holding the properties as keys and their counts as values.</returns>
        private static IDictionary<string, int> countProperties(IEnumerable<BsonDocument> infoboxes)
        {
            var counts = new Dictionary<string, int>();

            foreach (var doc in infoboxes)
            {
                foreach (var element in doc.Elements)
                {
                    if (counts.ContainsKey(element.Name))
                    { counts[element.Name]++; }
                    else
                    { counts[element.Name] = 1; }
                }
            }
            return counts;
        }

        /// <summary>
        /// Clean text passages extracted from Wikipedia.
        /// Removes Mediawiki and XML tags.
        /// </summary>
        /// <param name="text"></param>
        /// <returns></returns>
        public static string cleanWiki(string text)
        {
            // TODO: clean "&lt;ref name"

            // Extract dates from Mediawiki markup
            foreach (var regex in dateRegex)
            {
                var match = regex.Match(text);
                while (match.Success)
                {
                    text = text.Replace(match.Value, match.Groups["year"].Value + "-" + match.Groups["month"] + "-" + match.Groups["day"]);
                    match = match.NextMatch();
                }
            }

            // remove Wiki links and references
            foreach (var regex in cleaningRegex)
            {
                var match = regex.Match(text);
                while (match.Success)
                {
                    text = text.Replace(match.Groups[0].Value, match.Groups[1].Value);
                    match = match.NextMatch();
                }
            }

            // remove xml/html tags and links
            foreach (var regex in removingRegex)
            {
                var match = regex.Match(text);
                while (match.Success)
                {
                    text = text.Replace(match.Value, " ");
                    match = match.NextMatch();
                }
            }

            // resolve remaining xml entities
            text = text.Replace("&quot;", "\"").Replace("&amp;", "&").Replace("&ndash;", "--").Replace("&nbsp;", " ");

            return text.Trim();
        }

        /// <summary>
        /// Count the distinct values in the given list of dictionaries for the given key name.
        /// For each dictionary, the value for the key specified as property is extracted. The distinct values for that key are
        /// stored and counted.
        /// </summary>
        /// <param name="infoboxes">A list of dictionaries</param>
        /// <param name="property">The property to count the distinct values for.</param>
        /// <returns>A dictionary mapping the values to their frequencies.</returns>
        private static Dictionary<string, int> countValues(IList<IDictionary<string, string>> infoboxes, string property)
        {
            var counts = new Dictionary<string, int>(infoboxes.Count);

            foreach (var infobox in infoboxes)
            {
                if (infobox.ContainsKey(property))
                {
                    var value = infobox[property];
                    if (counts.ContainsKey(value))
                    { counts[value]++; }
                    else
                    { counts.Add(value, 1); }
                }
            }
            return counts;
        }

        /// <summary>
        /// Read all the types stored in the database, including their stemmed forms.
        /// </summary>
        /// <param name="limit">The maximum number of types to retrieve</param>
        /// <param name="dbname">The database name</param>
        /// <param name="collectionname">The collection name</param>
        /// <returns></returns>
        public static IEnumerable<string> getTypes(int limit = 0, string dbname = "cache", string collectionname = "types")
        {
            var collection = new MongoClient().GetServer().GetDatabase(dbname).GetCollection(collectionname);

            var cursor = collection.FindAllAs<BsonDocument>().SetLimit(limit);
            var types = new List<string>();

            foreach (var type in cursor)
            {
                types.Add(type["_id"].ToString());
                if (type["stemmed", null] != null)
                { types.Add(type["stemmed"].ToString()); }
            }
            return types;
        }

        /// <summary>
        /// Store the given instances (infoboxes) as separate documents in database.
        /// Only instances are stored that containg the property that is used as an id ("pagetitle").
        /// </summary>
        /// <param name="infoboxes">A list of dictionaries representing infoboxes</param>
        /// <param name="dbname">The database name</param>
        /// <param name="collectionname">The collection name</param>
        /// <returns>The number of actually stored documents</returns>
        static int storeInstancesToMongoDB(ICollection<IDictionary<string, string>> infoboxes, string dbname = "cache", string collectionname = "instances")
        {
            Debug.Print("Writing {0} instances do MongoDb '{1}' (collection name: '{2}')", infoboxes.Count, dbname, collectionname);
            var collection = new MongoClient().GetServer().GetDatabase(dbname).GetCollection(collectionname);
            var docs = new List<BsonDocument>(infoboxes.Count);
            var names = new List<string>(infoboxes.Count);
            var idProperty = "pagetitle";
            var stemmer = new EnglishStemmer();

            foreach (var infobox in infoboxes)
            {
                var doc = new BsonDocument();

                if (infobox.ContainsKey(idProperty))
                {
                    doc.Add("_id", infobox[idProperty]);
                    infobox.Remove(idProperty);
                    // add lowercased and stemmed versions of title:
                    doc.Add("lowercase", infobox[idProperty].ToLower());
                    doc.Add("stemmed", stem(infobox[idProperty], stemmer));
                }
                else
                {
                    Debug.Print("Ignoring infobox lacking property {0}.", idProperty);
                    continue;
                }

                foreach (var property in infobox.Keys)
                { doc.Add(cleanMongoKey(property), infobox[property]); }
                docs.Add(doc);
            }

            if (docs.Count > 0)
            { collection.InsertBatch(docs); }
            return docs.Count;
        }

        /// <summary>
        /// Reformat the text so that it can be used as a key in MongoDB.
        /// MongoDB keys cannot contain dots and must not start with '$'.
        /// </summary>
        /// <param name="text">The string to clean</param>
        /// <returns>The cleaned string</returns>
        static string cleanMongoKey(string text)
        {
            text = text.Replace('.', '_');
            if (text.StartsWith("$"))
            { text = text.Substring(1); }
            return text;
        }

        /// <summary>
        /// Find all the types in the database matching the given name, exactly or in its stemmed formd.
        /// </summary>
        /// <param name="typename">The type to match</param>
        /// <param name="dbname">The database name</param>
        /// <param name="collectionname">The collection name</param>
        /// <returns>An enumeration of matching types</returns>
        public static IEnumerable<string> findTypes(string typename, string dbname = "cache", string collectionname = "types")
        {
            Debug.Print("Searching types in database matching " + typename);
            var typecollection = new MongoClient().GetServer().GetDatabase(dbname).GetCollection(collectionname);
            var query = Query.Or(Query.EQ("_id", typename), Query.EQ("stemmed", stem(typename)));
            var typeCursor = typecollection.Find(query);
            var types = new List<string>();
            foreach (var type in typeCursor)
            {
                Debug.Print("Found type: " + type);
                types.Add(type["_id"].ToString());
            }
            return types;

        }

        /// <summary>
        /// For a given type, find the list of properties that are commonly shared by instances of that type.
        /// At first, all types matching the type are retrieved and a list of instances is retrieved matching these types.
        /// Within these instances, all existing properties are counted and the most frequent n properties are returned.
        /// </summary>
        /// <param name="type">The type of instances to consider</param>
        /// <param name="top">The maximum number of shared properties to return</param>
        /// <param name="limit">The maximum number of instances to consider as a sample</param>
        /// <param name="dbname">The database name</param>
        /// <param name="collectionname">The collection name</param>
        /// <returns>An enumeration of KeyValuePairs holding the properties and their respective counts.</returns>
        public static IEnumerable<KeyValuePair<string, int>> frequentProperties(string type, int top = 20, int limit = 100, string dbname = "cache", string collectionname = "instances")
        {
            IEnumerable<KeyValuePair<string, int>> result;
            Debug.Print("Searching for frequently used properties of type '{0}'.", type);
            var specialProperties = new String[] { "_id", "stemmed", "infoboxtype", "lowercase", "image", "caption", "logo", "company_logo", "map", "image_flag", "image_map", "logofile", "logosize" };
            var collection = new MongoClient().GetServer().GetDatabase(dbname).GetCollection(collectionname);
            // find all types in database matching the given type:
            var types = findTypes(type);

            if (types.Count() == 0)
            { result = new List<KeyValuePair<string, int>>(); }
            else
            {
                var queries = new List<IMongoQuery>(types.Count() * typefields.Length);
                foreach (var field in typefields)
                {
                    foreach (var typename in types)
                    {
                        Debug.Print("Querying for {0} in field {1}", typename, field);
                        queries.Add(Query.EQ(field, typename));
                    }
                }
                var docs = collection.Find(Query.Or(queries)).SetLimit(limit);
                var counts = countProperties(docs).ToList();
                counts.RemoveAll(prop => specialProperties.Contains(prop.Key));

                counts.Sort((prop1, prop2) =>
                    {
                        return prop1.Value.CompareTo(prop2.Value);
                    });
                result = counts.Skip(counts.Count - top);
            }
            return result;
        }

        /// <summary>
        /// From a list of KeyValuePairs, returns the keys.
        /// </summary>
        /// <typeparam name="T">Type of the keys</typeparam>
        /// <typeparam name="U">Type of the values (not used)</typeparam>
        /// <param name="pairlist">A List of KeyValueParis</param>
        /// <returns>A list of the same class as the keys in the input list</returns>
        public static IEnumerable<T> keys<T, U>(IEnumerable<KeyValuePair<T, U>> pairlist)
        {
            var results = new List<T>();
            foreach (var pair in pairlist)
            {
                results.Add(pair.Key);
            }
            return results;
        }

        /// <summary>
        /// Checks if the given page exists (exactly or in a stemmed form) and returns the exact page name.
        /// If it doesn't exist, checks if a redirect exists and returns the target's page name.
        /// </summary>
        /// <param name="id">The page to look up.</param>
        /// <param name="dbname">Name of the database to use.</param>
        /// <param name="collectionname">Name of the collection to use.</param>
        /// <returns>A string containing the exact page name (same as input id if it exists in the exact spelling)</returns>
        public static string pageExists(string id, string dbname = "cache", string collectionname = "instance")
        {
            string pagename = null;
            BsonDocument doc;
            var collection = new MongoClient().GetServer().GetDatabase(dbname).GetCollection(collectionname);
            var query = Query.Or(Query.EQ("_id", id), Query.EQ("lowercase", id.ToLower()), Query.EQ("redirect", id));
            doc = collection.FindOne(query);

            if (doc == null)
            { doc = collection.FindOne(Query.EQ("stemmed", stem(id))); }

            if (doc != null)
            {
                if (doc["redirect", null] != null)
                { pagename = doc["redirect"].ToString(); }
                else
                { pagename = doc["_id"].ToString(); }
            }
            return pagename;
        }

        /// <summary>
        /// Find a Wikipedia page in the database with the given title (_id) or, if available, in its stemmed form in the 'stemmed' field.
        /// If a redirect is found, it is followed and the target page is returned, if it exists.
        /// Optionally, the titles are searched for similar strings using regular expression search (slow).
        /// </summary>
        /// <param name="id">The exact page name to search for.</param>
        /// <param name="dbname">The database name</param>
        /// <param name="collectionname">The collection name</param>
        /// <param name="findSimilar">If true, similar titles are searched using regular expressions (slow)!</param>
        /// <returns>A Document containing all the fields extracted from the page infobox or null.</returns>
        public static BsonDocument findPage(string id, string dbname = "cache", string collectionname = "instances", bool findSimilar = true)
        {
            var collection = new MongoClient().GetServer().GetDatabase(dbname).GetCollection(collectionname);
            var query = Query.EQ("_id", id);
            var doc = collection.FindOne(query);
            if (doc == null)
            { doc = collection.FindOne(Query.EQ("stemmed", stem(id))); }

            if (findSimilar)
            {
                Regex[] regexes = { new Regex(@"^" + Regex.Escape(id) + @"$", RegexOptions.IgnoreCase),
                                new Regex(@"(^" + Regex.Escape(id) + @")|(" + Regex.Escape(id) + "$)", RegexOptions.IgnoreCase),
                                new Regex(@"\s"+Regex.Escape(id)+@"[\s\p{P}]",RegexOptions.IgnoreCase)};
                foreach (var regex in regexes)
                {
                    if (doc == null)
                    {
                        Debug.Print("No Wiki page {0} known; searching with regular expression {1}.", id, regex.ToString());
                        query = Query.Matches("_id", BsonRegularExpression.Create(regex));
                        doc = collection.FindOne(query);
                    }
                    else
                    { break; }
                }
            }

            if (doc != null && doc["redirect", null] != null)
            {
                Debug.Print("Following redirect from '{0}' to '{1}'", doc["_id"], doc["redirect"]);
                doc = collection.FindOne(Query.EQ("_id", doc["redirect"]));
            }
            return doc;
        }

        /// <summary>
        /// Find all entries that contain the given term in any of the given fields.
        /// In addition, the term is search in its stemmed form in the 'stemmed' field.
        /// </summary>
        /// <param name="fields">The list of fields to search the term in</param>
        /// <param name="term">The term to search</param>
        /// <param name="dbname">The database name</param>
        /// <param name="collectionname">The collection name</param>
        /// <returns>A list of IDs identifying the matching entries</returns>
        public static IEnumerable<string> findPages(IEnumerable<string> fields, string term, string dbname = "cache", string collectionname = "instances")
        {
            Debug.Print("Searching for {0} in collection {1}", String.Join(", ", fields), collectionname);
            var result = new List<string>();
            var collection = new MongoClient().GetServer().GetDatabase(dbname).GetCollection(collectionname);
            var queries = new List<IMongoQuery>(fields.Count() + 1);

            foreach (var field in fields)
            { queries.Add(Query.EQ(field, term)); }
            queries.Add(Query.EQ("stemmed", stem(term)));

            var query = Query.Or(queries);

            foreach (var doc in collection.Find(query))
            { result.Add(doc["_id"].ToString()); }

            return result;
        }

        /// <summary>
        /// Stem the given term(s).
        /// If the text comprises multiple tokens, it is tokenized and each token is stemmed separately.
        /// </summary>
        /// <param name="text">The text to stem.</param>
        /// <param name="stemmer">The stemmer to use. If non is given, an English stemmer is used</param>
        /// <returns>The string in its stemmed form</returns>
        public static string stem(string text, IStemmer stemmer = null)
        {
            if (stemmer == null)
            { stemmer = new EnglishStemmer(); }

            var stemmed = new StringBuilder(text.Length);

            foreach (var token in QueryParser.QuestionParser.tokenize(text))
            { stemmed.Append(stemmer.Stem(token) + " "); }
            stemmed.Length--;

            return stemmed.ToString();
        }

        /// <summary>
        /// For the given collection, create a 'stemmed' field for every instance to contain the stemmed version of the '_id' field.
        /// </summary>
        /// <param name="collectionname">The collection to stem</param>
        /// <param name="limit">The maximum number of instances to stem</param>
        /// <param name="dbname">The database name</param>
        /// <param name="fieldname">The field to store stems in</param>
        /// <param name="ensureIndex">Create an index over the stem field?</param>
        public static void stemCollection(string collectionname, int limit = 0, string dbname = "cache", string fieldname = "stemmed", bool ensureIndex = true)
        {
            var collection = new MongoClient().GetServer().GetDatabase(dbname).GetCollection(collectionname);
            var stemmer = new EnglishStemmer();

            var cursor = collection.Find(Query.NotExists(fieldname)).SetLimit(limit);
            foreach (var doc in cursor)
            {
                var id = doc["_id"];
                var stemmed = stem(id.ToString(), stemmer);
                if (stemmed != id.ToString())
                {
                    var update = Update.Set(fieldname, new BsonString(stemmed));
                    collection.Update(Query.EQ("_id", id), update);
                }
            }
            if (ensureIndex)
            { collection.EnsureIndex(fieldname); }
        }
    }
}
