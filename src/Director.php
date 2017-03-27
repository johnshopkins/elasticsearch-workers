<?php

namespace ElasticsearchWorkers;

use Secrets\Secret;

class Director
{
	public function __construct($settings = array())
	{
    // logger exchange
		$this->logger = $settings["logger"];

    // plugin namespace (hub, hubapi, jhu)
		$this->namespace = $settings["namespace"];

    // function that tests whether an item can be pushed to elasticsearch
    // arguments: id, type
		$this->saveTest = $settings["saveTest"];

    $this->setupGearmanClient($settings["servers"]);
	}

  protected function setupGearmanClient($servers)
  {
    $this->gearmanClient = new \GearmanClient();

    foreach ($servers as $server) {
      $this->gearmanClient->addServer($server->hostname);
    }
  }

	public function saved($id, $type)
	{
    $saved = call_user_func($this->saveTest, $id);

    if ($saved) {
      $this->put($id, $type);
    } else {
      $this->remove($id, $type);
    }
	}

	public function put($id, $type)
	{
		return $this->gearmanClient->doBackground("{$this->namespace}_elasticsearch_put", json_encode(array(
			"id" => $id,
			"type" => $type
		)));
	}

	public function remove($id, $type)
	{
    return $this->gearmanClient->doBackground("{$this->namespace}_elasticsearch_delete", json_encode(array(
      "id" => $id,
			"type" => $type
		)));
	}

	public function reindex()
	{
		return $this->gearmanClient->doNormal("{$this->namespace}_elasticsearch_reindex", json_encode(array()));
	}

}
