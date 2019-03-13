using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

[assembly : InternalsVisibleTo("CassandraPrimitives.Tests")]

namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog.Linq
{
    internal static class MoreLinq
    {
        public static IEnumerable<IEnumerable<TSource>> Batch<TSource>(
            this IEnumerable<TSource> source,
            int size)
        {
            return source.Batch(size, x => x);
        }

        public static IEnumerable<TResult> Batch<TSource, TResult>(
            this IEnumerable<TSource> source,
            int size,
            Func<IEnumerable<TSource>, TResult> resultSelector)
        {
            if(source == null)
                throw new ArgumentNullException(nameof(source));
            if(size <= 0)
                throw new ArgumentOutOfRangeException(nameof(size));
            if(resultSelector == null)
                throw new ArgumentNullException(nameof(resultSelector));
            return _();

            IEnumerable<TResult> _()
            {
                TSource[] array = null;
                int newSize = 0;
                foreach(TSource value in source)
                {
                    if(array == null)
                        array = new TSource[size];
                    array[newSize++] = value;
                    if(newSize == size)
                    {
                        yield return resultSelector(array);
                        array = null;
                        newSize = 0;
                    }
                }
                if(array != null && newSize > 0)
                {
                    Array.Resize(ref array, newSize);
                    yield return resultSelector(array);
                }
            }
        }

        public static IEnumerable<T> Pipe<T>(this IEnumerable<T> source, Action<T> action)
        {
            if(source == null)
                throw new ArgumentNullException(nameof(source));
            if(action == null)
                throw new ArgumentNullException(nameof(action));
            return _();

            IEnumerable<T> _()
            {
                foreach(T obj in source)
                {
                    action(obj);
                    yield return obj;
                }
            }
        }

        public static void ForEach<T>(this IEnumerable<T> source, Action<T> action)
        {
            if(source == null)
                throw new ArgumentNullException(nameof(source));
            if(action == null)
                throw new ArgumentNullException(nameof(action));
            foreach(T obj in source)
                action(obj);
        }

        public static TSource MaxBy<TSource, TKey>(
            this IEnumerable<TSource> source,
            Func<TSource, TKey> selector)
        {
            return source.MaxBy(selector, null);
        }

        public static TSource MaxBy<TSource, TKey>(
            this IEnumerable<TSource> source,
            Func<TSource, TKey> selector,
            IComparer<TKey> comparer)
        {
            if(source == null)
                throw new ArgumentNullException(nameof(source));
            if(selector == null)
                throw new ArgumentNullException(nameof(selector));
            comparer = comparer ?? Comparer<TKey>.Default;
            using(IEnumerator<TSource> enumerator = source.GetEnumerator())
            {
                if(!enumerator.MoveNext())
                    throw new InvalidOperationException("Sequence contains no elements");
                TSource source1 = enumerator.Current;
                TKey y = selector(source1);
                while(enumerator.MoveNext())
                {
                    TSource current = enumerator.Current;
                    TKey x = selector(current);
                    if(comparer.Compare(x, y) > 0)
                    {
                        source1 = current;
                        y = x;
                    }
                }
                return source1;
            }
        }
    }
}