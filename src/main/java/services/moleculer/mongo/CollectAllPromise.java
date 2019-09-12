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

import static services.moleculer.mongo.MongoDAO.COUNT;
import static services.moleculer.mongo.MongoDAO.ROWS;

import java.util.LinkedList;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.datatree.Promise;
import io.datatree.Tree;

public class CollectAllPromise<T> extends Promise implements Subscriber<T> {

	// --- VARIABLES ---

	protected LinkedList<T> values = new LinkedList<>();

	// --- CONSTRUCTOR ---

	public CollectAllPromise(Publisher<T> publisher) {
		publisher.subscribe(this);
	}

	// --- SUBSCRIBER METHODS ---

	@Override
	public void onSubscribe(Subscription subscription) {
		subscription.request(Integer.MAX_VALUE);
	}

	@Override
	public void onNext(T value) {
		values.addLast(value);
	}

	@Override
	public void onError(Throwable error) {
		root.completeExceptionally(error);
	}

	@Override
	public void onComplete() {
		Tree res = new Tree();
		res.put(COUNT, values.size());
		res.putObject(ROWS, values);
		root.complete(res);
	}

}