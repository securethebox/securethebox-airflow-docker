class LivyBatchOperator(SimpleHttpOperator):
    template_fields = ('args',)
    ui_color = '#f4a460'

    @apply_defaults
    def __init__(self,
                name,
                className,
                file,
                executorMemory='1g',
                driverMemory='512m',
                driverCores=1,
                executorCores=1,
                numExecutors=1,
                args=[],
                conf={},
                timeout=120,
                http_conn_id='apache_livy',
                *arguments, **kwargs):
        """
        If xcom_push is True, response of an HTTP request will also
        be pushed to an XCom.
        """
        super(LivyBatchOperator, self).__init__(
            endpoint='batches', *arguments, **kwargs)

        self.http_conn_id = http_conn_id
        self.method = 'POST'
        self.endpoint = 'batches'
        self.name = name
        self.className = className
        self.file = file
        self.executorMemory = executorMemory
        self.driverMemory = driverMemory
        self.driverCores = driverCores
        self.executorCores = executorCores
        self.numExecutors = numExecutors
        self.args = args
        self.conf = conf
        self.timeout = timeout
        self.poke_interval = 10

    def execute(self, context):
        """
        Executes the task
        """

        payload = {
            "name": self.name,
            "className": self.className,
            "executorMemory": self.executorMemory,
            "driverMemory": self.driverMemory,
            "driverCores": self.driverCores,
            "executorCores": self.executorCores,
            "numExecutors": self.numExecutors,
            "file": self.file,
            "args": self.args,
            "conf": self.conf
        }
        print (payload)
        headers = {
            'X-Requested-By': 'airflow',
            'Content-Type': 'application/json'
        }

        http = HttpHook(self.method, http_conn_id=self.http_conn_id)

        self.log.info("Submitting batch through Apache Livy API")

        response = http.run(self.endpoint,
                            json.dumps(payload),
                            headers,
                            self.extra_options)

        # parse the JSON response
        obj = json.loads(response.content)

        # get the new batch Id
        self.batch_id = obj['id']

        log.info('Batch successfully submitted with Id %s', self.batch_id)

        # start polling the batch status
        started_at = datetime.utcnow()
        while not self.poke(context):
            if (datetime.utcnow() - started_at).total_seconds() > self.timeout:
                raise AirflowSensorTimeout('Snap. Time is OUT.')

            sleep(self.poke_interval)

        self.log.info("Batch %s has finished", self.batch_id)

    def poke(self, context):
        '''
        Function that the sensors defined while deriving this class should
        override.
        '''

        http = HttpHook(method='GET', http_conn_id=self.http_conn_id)

        self.log.info("Calling Apache Livy API to get batch status")

        # call the API endpoint
        endpoint = 'batches/' + str(self.batch_id)
        response = http.run(endpoint)

        # parse the JSON response
        obj = json.loads(response.content)

        # get the current state of the batch
        state = obj['state']

        # check the batch state
        if (state == 'starting') or (state == 'running'):
            # if state is 'starting' or 'running'
            # signal a new polling cycle
            self.log.info('Batch %s has not finished yet (%s)',
                        self.batch_id, state)
            return False
        elif state == 'success':
            # if state is 'success' exit
            return True
        else:
            # for all other states
            # raise an exception and
            # terminate the task
            raise AirflowException(
                'Batch ' + str(self.batch_id) + ' failed (' + state + ')')