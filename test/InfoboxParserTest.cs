using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using InfoboxParser;

namespace InfoboxParserTests
{
    [TestClass]
    public class InfoboxParserTest
    {
        [TestMethod]
        public void Extract_Single_Property()
        {
            var line = "|name=John Doe";
            var properties = InfoboxParser.InfoboxParser.extractProperties(line);
            Assert.IsTrue(properties.Count == 1);
            Assert.IsTrue(properties.ContainsKey("name"));
            Assert.AreEqual("John Doe", properties["name"]);
        }

        [TestMethod]
        public void Extract_Birth_Date()
        {
            var line = "|birth_date = {{birth date|1900|10|1}}";
            var properties = InfoboxParser.InfoboxParser.extractProperties(line);
            Assert.IsTrue(properties.Count == 1);
            Assert.IsTrue(properties.ContainsKey("birth_date"));
            Assert.AreEqual("1900-10-1", properties["birth_date"]);
        }

        [TestMethod]
        public void Extract_Two_Properties()
        {
            var line = "|name=John Doe|type =   person";
            var properties = InfoboxParser.InfoboxParser.extractProperties(line);
            Assert.IsTrue(properties.Count == 2);
            Assert.IsTrue(properties.ContainsKey("name"));
            Assert.IsTrue(properties.ContainsKey("type"));
            Assert.AreEqual("John Doe", properties["name"]);
            Assert.AreEqual("person", properties["type"]);

        }

    }
}
