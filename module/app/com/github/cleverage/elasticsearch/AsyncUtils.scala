package com.github.cleverage.elasticsearch
import org.elasticsearch.action.{ActionListener, ActionRequest, ActionRequestBuilder, ActionResponse}
import play.libs.F

import scala.concurrent.{Future, Promise}

/**
  * Utils for managing Asynchronous tasks
  */
object AsyncUtils {
  /**
    * Create a default promise
    * @return
    */
  def createPromise[T](): Promise[T] = Promise[T]()

  /**
    * Execute an Elasticsearch request asynchronously
    * @param requestBuilder
    * @return
    */
  def executeAsync[RQ <: ActionRequest,RS <: ActionResponse, RB <: ActionRequestBuilder[RQ,RS]](requestBuilder: ActionRequestBuilder[RQ,RS]): Future[RS] = {
    val promise = Promise[RS]()

    requestBuilder.execute(new ActionListener[RS] {
      def onResponse(response: RS) {
        promise.success(response)
      }

      def onFailure(t: Throwable) {
        promise.failure(t)
      }

      override def onFailure(e: Exception): Unit = ???
    })

    promise.future
  }

  /**
    * Execute an Elasticsearch request asynchronously and return a Java Promise
    * @param requestBuilder
    * @return
    */
  def executeAsyncJava[RQ <: ActionRequest,RS <: ActionResponse, RB <: ActionRequestBuilder[RQ,RS]](requestBuilder: ActionRequestBuilder[RQ,RS]): F.Promise[RS] = {
    F.Promise.wrap(executeAsync(requestBuilder))
  }

}