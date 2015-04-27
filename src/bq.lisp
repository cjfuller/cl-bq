(in-package :cl-user)

(defpackage :bq
  (:use
   :cl-json
   :cjf-stdlib
   :cl-interpol)
  (:export
   :defquery
   :get-job-status
   :run-query-sync
   :run-query-async
   :wait-job
   :run-in-order
   :start-load-job
   :table-exists-p
   :bq-error
   :*project*
   :*output-dataset*
   :*account*
   :*gcloud-path*))

(in-package :bq)

(enable-interpol-syntax)

;; google cloud project id against which queries are made
(defparameter *project* "")

;; default dataset in which to store queries
(defparameter *output-dataset* "")

;; account (i.e. e-mail) by whom requests are being made
;; Used to activate the refresh token as needed.
(defparameter *account* "")

;; Path to the google cloud sdk bin dir.
(defparameter *gcloud-path*
  #?"${(namestring (user-homedir-pathname))}/google-cloud-sdk/bin/")

(define-condition bq-error (error)
  ((msg :initarg :msg :reader msg)))

(defun credentials ()
  (decode-json-from-source
   (pathname #?"${(namestring (user-homedir-pathname))}/.config/gcloud/credentials")))

(defun token ()
  (mget* (car (mget (credentials) :data)) :credential :access--token))

(defun refresh-token ()
  (mget* (car (mget (credentials) :data)) :credential :refresh--token))

(defun try-to-refresh (acct)
  (inferior-shell:run
   #?"${*gcloud-path*}/gcloud auth activate-refresh-token ${acct} ${(refresh-token)}"))

(defun table-exists-p (dataset table)
  "Does the specified table exist in the specified dataset?"
  ;; TODO: use the API rather than this silly run a shell command approach.
  (and (cl-ppcre:scan
        #?/\s+${table}\s*$/
        (inferior-shell:run
         #?"${*gcloud-path*}/bq ls -n 1000 ${dataset}" :output :string)
        :multi-line-mode :start)
       t))

(defmacro defquery (name &key (dataset *output-dataset*) table query)
  "Define a query (function) that can be run via run-query-sync.

   required kwargs:
       table: the table into which the output will be written
       query: a string containing the query to run
   optional kwarg:
       dataset: the dataset in which the output table will be created.  Defaults
       to *output-dataset*.
"
  ;; TODO: this feels like a gratuitous macro.  Think more about the query
  ;; interface.
  `(defun ,name ()
     (:dataset ,dataset
      :table ,table
      :query ,query)))

(defun jobs-url ()
  #?"https://www.googleapis.com/bigquery/v2/projects/${*project*}/jobs")

(defun auth-headers ()
  `((:|Authorization| . ,#?"Bearer ${(token)}")))

; Don't print all the headers with the bearer token to stdout.
(setf drakma:*header-stream* (make-broadcast-stream))

(defun query-job-config (query dataset table)
  "Format a json string to use as the request body for the query API call."
  (encode-json-plist-to-string
   `(:configuration
        ((:query .
            ((:allow-large-results . t)
             (:destination-table
              (:dataset-id . ,dataset)
              (:project-id . ,*project*)
              (:table-id . ,table))
             (:query . ,query)
             (:write-disposition . "WRITE_TRUNCATE")))))))

(defun get-job-status (job-id)
  "Get the status of the specifed job from the bigquery API.

   Args:
       job-id [string]: the bigquery job id to check
   Return:
       A list of the status ('DONE', 'PENDING' or 'RUNNING') and error message
       (nil if no error).
"
  (let* ((response
           (decode-json
            (drakma:http-request #?"${(jobs-url)}/${job-id}"
                                 :method :get
                                 :want-stream t
                                 :additional-headers (auth-headers))))
         (status (mget* response :status :state))
         (err-msg (mget* response :status :error-result)))
    (list status err-msg)))

(defun insert-job-async (config &key (retry t))
  "Insert a job with the specified configuration.  Return immediately.

   Args:
       config: a JSON string that is a valid job resource as specified by the
           bigquery API.
       retry: if truthy, will try to refresh credentials once via the refresh
           token if a 400 is encountered.
   Return:
       A string containing the bigquery job id.

   Conditions:
       Signals bq-error if an error is encountered while creating the job.
"
  (let* ((response (decode-json
                    (drakma:http-request
                     (jobs-url)
                     :method :post
                     :want-stream t
                     :additional-headers (auth-headers)
                     :content-type "application/json; charset=UTF-8"
                     :content config)))
         (job-id (mget* response :job-reference :job-id)))
    (if (mget response :error)
        (if (and retry (= 400 (mget* response :error :code)))
            (progn
              (try-to-refresh *account*)
              (insert-job-async config :retry nil))
            (error 'bq-error :msg (format nil "~a" response)))
        job-id)))

(defun insert-job-sync (config)
  "Insert a job via insert-job-async and wait for completion."
  (wait-job (insert-job-async config) 1))

(defun run-query-sync (qfun)
  "Run a query synchronously.

   Args:
       qfun: a query function as defined by defquery.
"
  (let* ((q-obj (funcall qfun))
         (q (regex-replace-all
             #?/\s+/
             (regex-replace-all #?/\s*--.*\n/ (mget q-obj :query) "")
             " "))
         (json-body (query-job-config
                     q
                     (mget q-obj :dataset)
                     (mget q-obj :table))))
    (println "Running query: ")
    (println q)
    (println #?"--> ${*output-dataset*}.${dest}\n\n\n")
    (insert-job-sync json-body)
    (princ "\n\n\n")))

(defun backoff-time (init-time)
  (let ((new-time (* 2 init-time)))
    (if (> new-time 15)
        15
        new-time)))

(defun wait-job (job-id poll-time)
  "Wait for the specified job to complete.

   Args:
       job-id [string]: the id of the bigquery job to wait for
       poll-time [number]: the initial time in seconds (max 15) after which to
           poll for completion.
"
  (destructuring-bind (status err-msg) (get-job-status job-id)
    (println #?"Waiting for job ${job-id}.  Status is: ${status}.")
    (cond
      ((equal status "DONE")
       (if err-msg
           (error 'bq-error :msg (format nil "~a" err-msg))
           nil))
      (t (progn
           (println (format nil "Will retry in ~a s..." poll-time))
           (sleep poll-time)
           (wait-job job-id (backoff-time poll-time)))))))

(defun load-job-config (gs-fn dest-dataset dest-table)
  (encode-json-plist-to-string
                     `(:configuration
                       ((:load
                         (:allow-large-results . t)
                         (:destination-table
                          (:dataset-id . ,dest-dataset)
                          (:project-id . ,*project*)
                          (:table-id . ,dest-table))
                         (:source-format . "DATASTORE_BACKUP")
                         (:source-uris ,gs-fn)
                         (:write-disposition . "WRITE_TRUNCATE"))))))

(defun start-load-job (gs-fn dest-dataset dest-table)
  (insert-job-sync (load-job-config gs-fn dest-dataset dest-table)))

(defun run-in-order (qlist)
    (map #'run-query-sync qlist))


