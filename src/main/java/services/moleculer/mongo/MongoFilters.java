/**
 * THIS SOFTWARE IS LICENSED UNDER MIT LICENSE.<br>
 * <br>
 * Copyright 2019 Andras Berkes [andras.berkes@programmer.net]<br>
 * Based on Moleculer Framework for NodeJS [https://moleculer.services].
 * <br><br>
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:<br>
 * <br>
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.<br>
 * <br>
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package services.moleculer.mongo;

import static services.moleculer.mongo.MongoDAO.ID;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.TextSearchOptions;
import com.mongodb.client.model.geojson.Geometry;
import com.mongodb.client.model.geojson.Point;

import io.datatree.Tree;

public class MongoFilters {

	// --- LOGICAL OPERATORS FOR FILTERS ---

	/**
	 * Creates a filter that preforms a logical OR of the provided list of
	 * filters.
	 *
	 * @param filters
	 *            the list of filters to and together
	 * @return the filter
	 */
	protected Tree or(Tree... filters) {
		return createFilter(Filters.or(toBsonIterable(filters)));
	}

	/**
	 * Creates a filter that performs a logical AND of the provided list of
	 * filters. Note that this will only generate a "$and" operator if
	 * absolutely necessary, as the query language implicity ands together all
	 * the keys. In other words, a query expression like:
	 *
	 * <blockquote>
	 * 
	 * <pre>
	 * and(eq("x", 1), lt("y", 3))
	 * </pre>
	 * 
	 * </blockquote>
	 *
	 * will generate a MongoDB query like: <blockquote>
	 * 
	 * <pre>
	 *    {x : 1, y : {$lt : 3}}
	 * </pre>
	 * 
	 * </blockquote>
	 *
	 * @param filters
	 *            the list of filters to and together
	 * @return the filter
	 */
	protected Tree and(Tree... filters) {
		return createFilter(Filters.and(toBsonIterable(filters)));
	}

	/**
	 * Creates a filter that matches all documents where the value of a field is
	 * an array that contains all the specified values.
	 *
	 * @param fieldName
	 *            the field name
	 * @param values
	 *            the list of values
	 * @return the filter
	 */
	protected Tree all(String fieldName, Object... values) {
		return createFilter(Filters.all(fieldName, toObjectIterable(values)));
	}

	/**
	 * Creates a filter that performs a logical NOR operation on all the
	 * specified filters.
	 *
	 * @param filters
	 *            the list of values
	 * @return the filter
	 */
	protected Tree nor(Tree... filters) {
		return createFilter(Filters.nor(toBsonIterable(filters)));
	}

	/**
	 * Creates a filter that matches all documents that do not match the passed
	 * in filter. Requires the field name to passed as part of the value passed
	 * in and lifts it to create a valid "$not" query:
	 *
	 * <blockquote>
	 * 
	 * <pre>
	 * not(eq("x", 1))
	 * </pre>
	 * 
	 * </blockquote>
	 *
	 * will generate a MongoDB query like: <blockquote>
	 * 
	 * <pre>
	 *    {x : $not: {$eq : 1}}
	 * </pre>
	 * 
	 * </blockquote>
	 *
	 * @param filter
	 *            the value
	 * @return the filter
	 */
	protected Tree not(Tree filter) {
		return createFilter(Filters.not(toBson(filter)));
	}

	/**
	 * Creates a filter that matches all documents where the "_id" field equals
	 * the specified document identifier.
	 * 
	 * @param id
	 *            document identifier (value of the "_id" field)
	 * @return the filter
	 */
	protected Tree eq(String id) {
		return createFilter(Filters.eq(ID, new ObjectId(id)));
	}

	/**
	 * Creates a filter that matches all documents where the value of the field
	 * name equals the specified value. Note that this doesn't actually generate
	 * a $eq operator, as the query language doesn't require it.
	 *
	 * @param fieldName
	 *            the field name
	 * @param value
	 *            the value, which may be null
	 * @return the filter
	 */
	protected Tree eq(String fieldName, Object value) {
		return createFilter(Filters.eq(fieldName, toObject(value)));
	}

	/**
	 * Creates a filter that matches all documents where the value of the field
	 * name does not equal the specified value.
	 *
	 * @param fieldName
	 *            the field name
	 * @param value
	 *            the value, which may be null
	 * @return the filter
	 */
	protected Tree ne(String fieldName, Object value) {
		return createFilter(Filters.ne(fieldName, toObject(value)));
	}

	/**
	 * Creates a filter that matches all documents where the value of a field
	 * equals any value in the list of specified values.
	 *
	 * @param fieldName
	 *            the field name
	 * @param values
	 *            the list of values
	 * @return the filter
	 */
	protected Tree in(String fieldName, Object... values) {
		return createFilter(Filters.in(fieldName, toObjectIterable(values)));
	}

	/**
	 * Creates a filter that matches all documents where the value of a field
	 * does not equal any of the specified values or does not exist.
	 *
	 * @param fieldName
	 *            the field name
	 * @param values
	 *            the list of values
	 * @return the filter
	 */
	protected Tree nin(String fieldName, Object... values) {
		return createFilter(Filters.nin(fieldName, toObjectIterable(values)));
	}

	/**
	 * Creates a filter that matches all documents where the value of the given
	 * field is less than the specified value.
	 *
	 * @param fieldName
	 *            the field name
	 * @param value
	 *            the value
	 * @return the filter
	 */
	protected Tree lt(String fieldName, Object value) {
		return createFilter(Filters.lt(fieldName, toObject(value)));
	}

	/**
	 * Creates a filter that matches all documents where the value of the given
	 * field is less than or equal to the specified value.
	 *
	 * @param fieldName
	 *            the field name
	 * @param value
	 *            the value
	 * @return the filter
	 */
	protected Tree lte(String fieldName, Object value) {
		return createFilter(Filters.lte(fieldName, toObject(value)));
	}

	/**
	 * Creates a filter that matches all documents where the value of the given
	 * field is greater than the specified value.
	 *
	 * @param fieldName
	 *            the field name
	 * @param value
	 *            the value
	 * @return the filter
	 */
	protected Tree gt(String fieldName, Object value) {
		return createFilter(Filters.gt(fieldName, toObject(value)));
	}

	/**
	 * Creates a filter that matches all documents where the value of the given
	 * field is greater than or equal to the specified value.
	 *
	 * @param fieldName
	 *            the field name
	 * @param value
	 *            the value
	 * @return the filter
	 */
	protected Tree gte(String fieldName, Object value) {
		return createFilter(Filters.gte(fieldName, toObject(value)));
	}

	/**
	 * Creates a filter that matches all documents that either contain the given
	 * field.
	 *
	 * @param fieldName
	 *            the field name
	 * @return the filter
	 */
	protected Tree exists(String fieldName) {
		return createFilter(Filters.exists(fieldName, true));
	}

	/**
	 * Creates a filter that matches all documents that either do not contain
	 * the given field.
	 *
	 * @param fieldName
	 *            the field name
	 * @return the filter
	 */
	protected Tree notExists(String fieldName) {
		return createFilter(Filters.exists(fieldName, false));
	}

	/**
	 * Creates a filter that matches all documents where the value of the field
	 * matches the given regular expression pattern with the given options
	 * applied.
	 *
	 * @param fieldName
	 *            the field name
	 * @param pattern
	 *            the pattern
	 * @return the filter
	 */
	protected Tree regex(String fieldName, String pattern) {
		return createFilter(Filters.regex(fieldName, pattern));
	}

	/**
	 * Creates a filter that matches all documents where the value of the field
	 * matches the given regular expression pattern with the given options
	 * applied.
	 *
	 * @param fieldName
	 *            the field name
	 * @param pattern
	 *            the pattern
	 * @param options
	 *            the options
	 * @return the filter
	 */
	protected Tree regex(String fieldName, String pattern, String options) {
		return createFilter(Filters.regex(fieldName, pattern, options));
	}

	/**
	 * Creates a filter that matches all documents for which the given
	 * expression is true.
	 *
	 * @param javaScriptExpression
	 *            the JavaScript expression
	 * @return the filter
	 */
	protected Tree where(String javaScriptExpression) {
		return createFilter(Filters.where(javaScriptExpression));
	}

	/**
	 * Creates a filter that matches all documents containing a field that is an
	 * array where at least one member of the array matches the given filter.
	 *
	 * @param fieldName
	 *            the field name
	 * @param filter
	 *            the filter to apply to each element
	 * @return the filter
	 */
	protected Tree elemMatch(String fieldName, Tree filter) {
		return createFilter(Filters.elemMatch(fieldName, toBson(filter)));
	}

	/**
	 * Allows the use of aggregation expressions within the query language.
	 *
	 * @param expression
	 *            the aggregation expression
	 * @return the filter
	 */
	protected Tree expr(Tree expression) {
		return createFilter(Filters.expr(toBson(expression)));
	}

	/**
	 * Creates a filter that matches all documents matching the given search
	 * term.
	 *
	 * @param search
	 *            the search term
	 * @return the filter
	 */
	protected Tree text(String search) {
		return createFilter(Filters.text(search));
	}

	/**
	 * Creates a filter that matches all documents matching the given the search
	 * term with the given text search options.
	 *
	 * @param search
	 *            the search term
	 * @param options
	 *            the text search options to use
	 * @return the filter
	 */
	protected Tree text(String search, TextSearchOptions options) {
		return createFilter(Filters.text(search, options));
	}

	/**
	 * Creates a filter that matches all documents containing a field with
	 * geospatial data that intersects with the specified shape.
	 *
	 * @param fieldName
	 *            the field name
	 * @param geometry
	 *            the bounding GeoJSON geometry object
	 * @return the filter
	 */
	protected Tree geoIntersects(String fieldName, Geometry geometry) {
		return createFilter(Filters.geoIntersects(fieldName, geometry));
	}

	/**
	 * Creates a filter that matches all documents containing a field with
	 * geospatial data that exists entirely within the specified shape.
	 *
	 * @param fieldName
	 *            the field name
	 * @param geometry
	 *            the bounding GeoJSON geometry object
	 * @return the filter
	 */
	protected Tree geoWithin(String fieldName, Geometry geometry) {
		return createFilter(Filters.geoWithin(fieldName, geometry));
	}

	/**
	 * Creates a filter that matches all documents containing a field with grid
	 * coordinates data that exist entirely within the specified box.
	 *
	 * @param fieldName
	 *            the field name
	 * @param lowerLeftX
	 *            the lower left x coordinate of the box
	 * @param lowerLeftY
	 *            the lower left y coordinate of the box
	 * @param upperRightX
	 *            the upper left x coordinate of the box
	 * @param upperRightY
	 *            the upper left y coordinate of the box
	 * @return the filter
	 */
	protected Tree geoWithinBox(String fieldName, double lowerLeftX, double lowerLeftY, double upperRightX,
			double upperRightY) {
		return createFilter(Filters.geoWithinBox(fieldName, lowerLeftX, lowerLeftY, upperRightX, upperRightY));
	}

	/**
	 * Creates a filter that matches all documents containing a field with grid
	 * coordinates data that exist entirely within the specified circle.
	 *
	 * @param fieldName
	 *            the field name
	 * @param x
	 *            the x coordinate of the circle
	 * @param y
	 *            the y coordinate of the circle
	 * @param radius
	 *            the radius of the circle, as measured in the units used by the
	 *            coordinate system
	 * @return the filter
	 */
	protected Tree geoWithinCenter(String fieldName, double x, double y, double radius) {
		return createFilter(Filters.geoWithinCenter(fieldName, x, y, radius));
	}

	/**
	 * Creates a filter that matches all documents containing a field with grid
	 * coordinates data that exist entirely within the specified polygon.
	 *
	 * @param fieldName
	 *            the field name
	 * @param points
	 *            a list of pairs of x, y coordinates. Any extra dimensions are
	 *            ignored
	 * @return the filter
	 */
	protected Tree geoWithinPolygon(String fieldName, List<List<Double>> points) {
		return createFilter(Filters.geoWithinPolygon(fieldName, points));
	}

	/**
	 * Creates a filter that matches all documents where the value of a field
	 * divided by a divisor has the specified remainder (i.e. perform a modulo
	 * operation to select documents).
	 *
	 * @param fieldName
	 *            the field name
	 * @param divisor
	 *            the modulus
	 * @param remainder
	 *            the remainder
	 * @return the filter
	 */
	protected Tree mod(String fieldName, long divisor, long remainder) {
		return createFilter(Filters.mod(fieldName, divisor, remainder));
	}

	/**
	 * Creates a filter that matches all documents containing a field with
	 * geospatial data that is near the specified GeoJSON point.
	 * 
	 * @param fieldName
	 *            the field name
	 * @param geometry
	 *            the bounding GeoJSON geometry object
	 * @param maxDistance
	 *            the maximum distance from the point, in meters. It may be
	 *            null.
	 * @param minDistance
	 *            the minimum distance from the point, in meters. It may be
	 *            null.
	 * @return the filter
	 */
	protected Tree near(String fieldName, Point geometry, Double maxDistance, Double minDistance) {
		return createFilter(Filters.near(fieldName, geometry, maxDistance, minDistance));
	}

	/**
	 * Creates a filter that matches all documents where the value of a field is
	 * an array of the specified size.
	 * 
	 * @param fieldName
	 *            the field name
	 * @param size
	 *            the size of the array
	 * @return the filter
	 */
	protected Tree size(String fieldName, int size) {
		return createFilter(Filters.size(fieldName, size));
	}

	/**
	 * Create a Tree object from a Bson filter.
	 * 
	 * @param bson
	 *            input Bson
	 * @return the filter
	 */
	protected Tree createFilter(Bson bson) {
		return new BsonTree(bson);
	}

	/**
	 * Converts Tree object to Bson object.
	 * 
	 * @param tree
	 *            input Tree object
	 * @return the Bson
	 */
	@SuppressWarnings("unchecked")
	protected Bson toBson(Tree tree) {
		if (tree == null) {
			return null;
		}
		Object o = tree.asObject();
		if (o == null) {
			return null;
		}
		if (o instanceof Bson) {
			return (Bson) o;
		}
		return new Document((Map<String, Object>) o);
	}

	// --- PRIVATE UTILITIES ---

	private Object toObject(Object value) {
		if (value != null && value instanceof Tree) {
			return ((Tree) value).asObject();
		}
		return value;
	}

	private Iterable<Object> toObjectIterable(Object... array) {
		if (array == null || array.length == 0) {
			return Collections.emptyList();
		}
		ArrayList<Object> list = new ArrayList<>(array.length);
		for (Object object : array) {
			list.add(toObject(object));
		}
		return list;
	}

	private Iterable<Bson> toBsonIterable(Tree[] array) {
		if (array == null || array.length == 0) {
			return Collections.emptyList();
		}
		ArrayList<Bson> list = new ArrayList<>(array.length);
		for (Tree tree : array) {
			list.add(toBson(tree));
		}
		return list;
	}

}
