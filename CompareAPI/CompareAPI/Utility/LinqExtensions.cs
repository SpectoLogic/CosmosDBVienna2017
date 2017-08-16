using CompareAPI.MongoDBDemo;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace CompareAPI.Linq
{
    public static class SpectoLogicLinqExpressionExtensions
    {
        public static Expression<TDelegate> AndAlso<TDelegate>(this Expression<TDelegate> left, Expression<TDelegate> right)
        {
            // return Expression.Lambda<TDelegate>(Expression.AndAlso(left, right), left.Parameters);
            return Expression.Lambda<TDelegate>(Expression.AndAlso(
                left.Body,
                new ExpressionParameterReplacer(right.Parameters, left.Parameters).Visit(right.Body)), left.Parameters
            );
        }

        /// <summary>
        /// This extention methods allows to define the OrderBy Criteria by passing in a string reference
        /// to the property which should be sorted by. Followed by " ASC" or " DESC" you can define if
        /// the ordering should be ascending or descending.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="source">Queryable Collection</param>
        /// <param name="property">e.g. "Firstname ASC" or "Lastname DESC"</param>
        /// <returns></returns>
        public static IOrderedQueryable<T> OrderBy<T>(this IQueryable<T> source, string property)
        {
            bool ascending = true;
            string[] sort = property.Split(' ');
            if (sort.Length > 1)
            {
                property = sort[0];
                if (sort[1].ToUpper() == "ASC") ascending = true;
                if (sort[1].ToUpper() == "DESC") ascending = false;
            }
            if (ascending)
                return ApplyOrder(source, property, "OrderBy");
            else
                return ApplyOrder(source, property, "OrderByDescending");
        }

        private static IOrderedQueryable<T> ApplyOrder<T>(IQueryable<T> source, string property, string methodName)
        {
            var props = property.Split('.');
            var type = typeof(T);
            var arg = Expression.Parameter(type, "x");
            Expression expr = arg;
            foreach (var prop in props)
            {
                // Figure out if the property was "renamed" with JsonProperty(PropertyName)
                // if so find the corresponding property
                PropertyInfo pi = null;
                var renamedProperties = type.GetProperties().Where(p =>
                {
                    IEnumerable<Object> re = p.GetCustomAttributes(false).Where(a => a is Newtonsoft.Json.JsonPropertyAttribute);
                    return re.Count() > 0 ? true : false;
                });
                foreach (var renprop in renamedProperties)
                {
                    Newtonsoft.Json.JsonPropertyAttribute jpa = (renprop.GetCustomAttributes(false).Where(a => a is Newtonsoft.Json.JsonPropertyAttribute)).FirstOrDefault() as Newtonsoft.Json.JsonPropertyAttribute;
                    if (jpa.PropertyName == prop) { pi = renprop; break; }
                }
                if (pi == null)
                    pi = type.GetProperty(prop);
                expr = Expression.Property(expr, pi);
                type = pi.PropertyType;
            }
            var delegateType = typeof(Func<,>).MakeGenericType(typeof(T), type);
            var lambda = Expression.Lambda(delegateType, expr, arg);

            var result = typeof(Queryable).GetMethods().Single(
                    method => method.Name == methodName
                            && method.IsGenericMethodDefinition
                            && method.GetGenericArguments().Length == 2
                            && method.GetParameters().Length == 2)
                    .MakeGenericMethod(typeof(T), type)
                    .Invoke(null, new object[] { source, lambda });
            return (IOrderedQueryable<T>)result;
        }

    }

    public class ExpressionParameterReplacer : ExpressionVisitor
    {
        public ExpressionParameterReplacer(IList<ParameterExpression> fromParameters, IList<ParameterExpression> toParameters)
        {
            ParameterReplacements = new Dictionary<ParameterExpression, ParameterExpression>();
            for (int i = 0; i != fromParameters.Count && i != toParameters.Count; i++)
                ParameterReplacements.Add(fromParameters[i], toParameters[i]);
        }
        private IDictionary<ParameterExpression, ParameterExpression> ParameterReplacements
        {
            get;
            set;
        }
        protected override Expression VisitParameter(ParameterExpression node)
        {
            if (ParameterReplacements.TryGetValue(node, out ParameterExpression replacement))
                node = replacement;
            return base.VisitParameter(node);
        }
    }

    public static class SpectoLogicCompareAPIListExtensions
    {
        public static void Assert(this List<MongoItem> result, string[] expectedResult)
        {
            if (result.Count != expectedResult.Length) throw new Exception("Result differs from expected result (amount of items).");
            for(int i=0;i<result.Count;i++)
            {
                if ($"{result[i].DemoUser.FirstName} {result[i].DemoUser.LastName}" != expectedResult[i])
                    throw new Exception("Result differs from expected result (wrong item)");
            }
        }
    }

}
