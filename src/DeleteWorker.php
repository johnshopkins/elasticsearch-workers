<?php

namespace ElasticsearchWorkers;

class DeleteWorker extends Base
{
  protected function addFunctions()
  {
    parent::addFunctions();
    $this->worker->addFunction("{$this->namespace}_elasticsearch_delete", array($this, "delete"));
  }

  public function delete(\GearmanJob $job)
  {
    $workload = json_decode($job->workload());

    $this->logger->addInfo("Initiating elasticsearch DELETE of post #{$workload->id}...");

    try {
      $result = $this->deleteOne($workload->id, $workload->type);
      if ($result) {
        $this->logger->addInfo("Finished elasticsearch DELETE of post #{$workload->id}.");
      }
    } catch (\Exception $e) {
      $error = $e->getMessage();
      $this->logger->addError("Delete of post {$workload->id} FAILED. Error message: {$error}.");
    }
  }
}
