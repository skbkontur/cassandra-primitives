using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.TestConfigurations
{
    public class ParametersParser
    {
        public static List<List<object>> Product(params List<object>[] lists)
        {
            return Product(0, lists);
        }

        private static List<List<object>> Product(int pos, params List<object>[] lists)
        {
            if (lists.Length == pos)
                return new List<List<object>> {new List<object>()};
            var subResults = Product(pos + 1, lists);
            var results = new List<List<object>>();
            foreach (var value in lists[pos])
                results.AddRange(subResults.Select(subResult => new List<object> {value}.Concat(subResult).ToList()));
            return results;
        }

        public static List<string> ParseStrings(string rawValue)
        {
            rawValue = Regex.Replace(rawValue, @"\s*", "");
            List<string> resultList;
            if (TryParseList(rawValue, out resultList))
                return resultList;
            if (TryParseTeamCityFormattedList(rawValue, out resultList))
                return resultList;
            return new List<string> {rawValue};
        }

        public static List<TEnum> ParseEnums<TEnum>(string parameterName, string rawValue)
            where TEnum : struct
        {
            var values = ParseStrings(rawValue);
            var result = new List<TEnum>();
            foreach (var value in values)
            {
                TEnum currentResult;
                if (!Enum.TryParse(value, out currentResult))
                    throw new Exception(string.Format("Invalid value of enum parameter {0}", parameterName));
                result.Add(currentResult);
            }
            return result;
        }

        public static List<Enum> ParseEnums(Type enumType, string parameterName, string rawValue)
        {
            var values = ParseStrings(rawValue);
            var result = new List<Enum>();
            foreach (var value in values)
            {
                try
                {
                    var currentResult = (Enum)Enum.Parse(enumType, value);
                    result.Add(currentResult);
                }
                catch (Exception)
                {
                    throw new Exception(string.Format("Invalid value of enum parameter {0}", parameterName));
                }
            }
            return result;
        }

        public static int ParseInt(string parameterName, string rawValue)
        {
            int result;
            if (!int.TryParse(rawValue, out result))
                throw new Exception(string.Format("Invalid value was given for parameter {0}", parameterName));
            return result;
        }

        public static List<int> ParseInts(string parameterName, string rawValue)
        {
            rawValue = Regex.Replace(rawValue, @"\s*", "");

            int result;
            if (int.TryParse(rawValue, out result))
                return new List<int> {result};
            List<int> resultList;
            if (TryParseRange(rawValue, out resultList))
                return resultList;
            if (TryParseListOfInts(rawValue, out resultList))
                return resultList;

            throw new Exception(string.Format("Invalid value of integer parameter {0}", parameterName));
        }

        private static bool TryParseRange(string source, out List<int> result)
        {
            source = Regex.Replace(source, @"\s*", "");
            var match = Regex.Match(source, @"^range\((\d+),(\d+),(\d+)\)$");
            if (match.Success)
            {
                var start = int.Parse(match.Groups[1].Value);
                var end = int.Parse(match.Groups[2].Value);
                var step = int.Parse(match.Groups[3].Value);
                if (start > end || step <= 0 || (end - start) / step > maxIterationsOfSingleParameter)
                {
                    result = null;
                    return false;
                }
                result = new List<int>();
                for (int i = start; i < end; i += step)
                    result.Add(i);
                return true;
            }
            result = null;
            return false;
        }

        private static bool TryParseListOfInts(string source, out List<int> result)
        {
            List<string> parsedTokens;
            if (!TryParseList(source, out parsedTokens) && !TryParseTeamCityFormattedList(source, out parsedTokens))
            {
                result = null;
                return false;
            }
            var ints = new List<int>();
            foreach (var token in parsedTokens)
            {
                int value;
                if (!int.TryParse(token, out value))
                {
                    result = null;
                    return false;
                }
                ints.Add(value);
            }
            result = ints;
            return true;
        }

        private static bool TryParseList(string source, out List<string> result)
        {
            source = Regex.Replace(source, @"\s*", "");
            if (source.StartsWith("[") && source.EndsWith("]"))
            {
                source = source.Substring(1, source.Length - 2);
                result = source.Split(',').ToList();
                return true;
            }
            result = null;
            return false;
        }

        private static bool TryParseTeamCityFormattedList(string source, out List<string> result)
        {
            source = Regex.Replace(source, @"\s*", "");
            result = source.Split('|').ToList();
            return true;
        }

        private const int maxIterationsOfSingleParameter = 100000;
    }
}