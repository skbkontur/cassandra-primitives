using System;
using System.Collections.Generic;

namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog.Commons
{
    public static class CatalogueEnumerable
    {
        public static IEnumerable<T> SortedMerge<T>(this IEnumerable<T> seq1, IEnumerable<T> seq2)
            where T : IComparable
        {
            return SortedMerge(seq1, seq2, (a, b) => a.CompareTo(b));
        }

        public static IEnumerable<T> SortedMerge<T>(this IEnumerable<T> seq1, IEnumerable<T> seq2, Func<T, T, int> compare)
        {
            if(seq1 == null) seq1 = new T[0];
            if(seq2 == null) seq2 = new T[0];
            var en1 = seq1.GetEnumerator();
            var en2 = seq2.GetEnumerator();
            if(!MoveNext(en1))
            {
                while(MoveNext(en2))
                    yield return en2.Current;
                yield break;
            }
            if(!MoveNext(en2))
            {
                yield return en1.Current;
                while(MoveNext(en1))
                    yield return en1.Current;
                yield break;
            }

            var l = en1.Current;
            var r = en2.Current;
            while(true)
            {
                var res = compare(l, r);
                if(res < 0)
                {
                    yield return l;
                    if(!MoveNext(en1))
                    {
                        yield return en2.Current;
                        while(MoveNext(en2))
                            yield return en2.Current;
                        yield break;
                    }
                    l = en1.Current;
                }
                else
                {
                    yield return r;
                    if(!MoveNext(en2))
                    {
                        yield return en1.Current;
                        while(MoveNext(en1))
                            yield return en1.Current;
                        yield break;
                    }
                    r = en2.Current;
                }
            }
        }

        private static bool MoveNext<T>(IEnumerator<T> enumerator)
        {
            try
            {
                return (enumerator.MoveNext());
            }
            catch(Exception e)
            {
                throw new CantMoveNextException(e);
            }
        }
    }
}