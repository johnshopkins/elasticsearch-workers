<?php

namespace ElasticsearchWorkers;

class PutWorker extends Base
{
  protected function addFunctions()
  {
    parent::addFunctions();
    $this->worker->addFunction("{$this->namespace}_elasticsearch_put", array($this, "put"));
  }

  public function put(\GearmanJob $job)
  {
    $workload = json_decode($job->workload());

    if (!in_array($workload->type, $this->types)) {
      $this->logger->addInfo("Type is not saved to ES");
      return;
    }

    $this->logger->addInfo("Initiating elasticsearch PUT of post #{$workload->id}...");

    try {
      $result = $this->putOne($workload->id, $workload->type);
      if ($result) {
        $this->logger->addInfo("Finished elasticsearch PUT of post #{$workload->id}.");
      }
    } catch (\Exception $e) {
      $error = $e->getMessage();
      $this->logger->addError("Put of post {$workload->id} FAILED. Error message: {$error}");
    }
  }

  /**
   * Put all fields of study present in
   * the WordPress database into elasticsearch
   * @return array $response Responses from elasticsearch
   */

  /**
   * Put all nodes whose content type are present in
   * the array of content types passed to this function
   * into the elasticsearch engine.
   * @param  [string] $index        Elasticsearch index to put content in
   */
  public function putAll($index)
  {
      $data = array();

      foreach ($this->types as $type) {

        $query = new \EntityFieldQuery();

        $query->entityCondition("entity_type", "node")
          ->entityCondition("bundle", $type)
          ->propertyCondition("status", NODE_PUBLISHED)
          ->fieldCondition("field_approved", "value", 1, "=");

        $result = $query->execute();

        if (!isset($result["node"])) continue;


        $ids = array_keys($result["node"]);
        $nodes = entity_load("node", $ids);


        foreach ($nodes as $node) {

          try {
            $this->putOne($node, $index);
            watchdog("elastic_posts", " Put of post {$type}/{$node->nid} complete.");
          } catch (\Exception $e) {
            $error = json_decode($e->getMessage());
            watchdog("elastic_posts", " Put of post {$type}/{$node->nid} FAILED. Error message: {$error->error}");
          }

        }
      }
  }

  public function putOne($id, $type)
  {
    $data = $this->getter->get($id, $type);

    $this->logger->addInfo("post", array($data));

    // data is not in the API
    if (!$data) {
      $this->logger->addInfo("Post #{$id} is not in the API; deleting...");
      return $this->deleteOne($id, $type);
    }

    $params = array(
      "index" => $this->index,
      "type" => $type,
      "id" => $id,
      "body" => $this->cleaner->clean($data, $type)
    );

    return $this->elasticsearchClient->index($params);
  }

  protected function getNode($id, $type)
  {
    $getterClass = "\\ElasticPosts\\Getters\\{$type}";
    $getter = new $getterClass();

    return $getter->get($id);
  }

  /**
   * Get the post data ready for elasticsearch
   * @param  object $post Post object
   * @return object Cleaned post
   */
  protected function cleanPost($post)
  {
    $condensedClass = str_replace("_", "", $post->type);
    $cleanerClass = "\\ElasticPosts\\Cleaners\\{$condensedClass}";
    if (!class_exists($cleanerClass)) {
        $cleanerClass = "\\ElasticPosts\\Cleaners\\Base";
    }
    $cleaner = new $cleanerClass();
    return $cleaner->clean($post);
  }
}
