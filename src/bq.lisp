(in-package :cl-user)

(defpackage :bq-api-client
  (:use
   :cl
   :cl-annot
   :cl-json
   :col
   :cl-interpol)
  (:export
   :*project*
   :*account*
   :*gcloud-path*
   :http-get
   :post
   :ls-tables
   :table-exists-p
   :job-status
   :insert-job
   :insert-query-job
   :insert-backup-load-job
   :insert-json-load-job
   :insert-export-to-csv-job
   :read-table-column-names
   :fetch-table-data))

(defpackage :bq
  (:use
   :cl
   :cl-annot
   :cl-ppcre
   :col
   :cl-interpol
   :ascii-table
   :bq-api-client)
  (:export
   :*default-dataset*
   :defquery
   :wait-on-job-completion
   :run-query-sync
   :select
   :read-table-data
   :preview-table-data
   :run-in-order))

(in-package :bq-api-client)

(enable-reader-exts)

(defparameter *project*
  "Google cloud project id against which queries are made")

(defparameter *account* ""
  "Google account (i.e. e-mail) by whom request are being made.  Used to
  activate refresh tokens as needed.")

(defparameter *gcloud-path*
  #?"${(namestring (user-homedir-pathname))}/google-cloud-sdk/bin/"
  "Path to the google cloud sdk bin directory.")

(defun base-url ()
  #?"https://www.googleapis.com/bigquery/v2/projects/${*project*}")

(defun api-url (partial)
  (<> (base-url) partial))

(defun auth-headers ()
  #M(:type :alist){
    :|Authorization| #?"Bearer ${(token)}"})

(defun credentials ()
  (-> (pathname #?"${(namestring (user-homedir-pathname))}/.config/gcloud/credentials")
    decode-json-from-source
    (mget :data)
    car
    (mget :credential)))

(defun token ()
  (mget (credentials) :access--token))

(defun refresh-token ()
  (mget (credentials) :refresh--token))

(defun refresh ()
  #!"${*gcloud-path*}/gcloud auth activate-refresh-token ${*account*} ${(refresh-token)}")

(defun http-get (relative &optional (retry-with-refresh t))
  (let* ((url (api-url relative))
         ; Don't print all the headers with the bearer token to stdout.
         (drakma:*header-stream* (make-broadcast-stream))
         (response (decode-json
                    (drakma:http-request url
                                         :method :get
                                         :want-stream t
                                         :additional-headers (auth-headers)))))
    (if (and retry-with-refresh
               (mget response :error)
               (= 401 (mget* response :error :code)))
        (progn (refresh)
               (http-get relative nil))
        response)))

(defun post (relative body &optional (retry-with-refresh t))
  (let* ((url (api-url relative))
         ; Don't print all the headers with the bearer token to stdout.
         (drakma:*header-stream* (make-broadcast-stream))
         (response (decode-json
                    (drakma:http-request url
                                         :method :post
                                         :want-stream t
                                         :additional-headers (auth-headers)
                                         :content-type "application/json; charset=UTF-8"
                                         :content (encode-json-to-string body)))))

    (if (and retry-with-refresh
               (mget response :error)
               (= 401 (mget* response :error :code)))
        (progn (refresh)
               (post relative body nil))
        response)))

(defun ls-tables (dataset &key (n 20))
  (-> (http-get #?"/datasets/${dataset}/tables?maxResults=${n}")
    (mget :tables)
    (rmapcar (lambda (tbl) (mget* tbl :table-reference :table-id)))))

(defun table-exists-p (dataset table)
  "Does the specified table exist in the specified dataset?"
  (member table (ls-tables dataset) :test #'equal))

(defun job-status (job-id)
  (-> (http-get #?"/jobs/${job-id}")
    (tee
     (mget* :status :state)
     (mget* :status :error-result))))

(defun query-job-config (query dataset table)
  #M{:configuration
     #M{
        :query
        #M{
           :allow-large-results t
           :flatten-results nil
           :destination-table
           #M{
              :project-id *project*
              :dataset-id dataset
              :table-id table}
           :query query
           :write-disposition "WRITE_TRUNCATE"}}})

(defun load-job-config (gs-fn dataset table &key (format "DATASTORE_BACKUP") (max-bad 0))
  #M{:configuration
     #M{
        :load
        #M{
           :destination-table
           #M{
              :project-id *project*
              :dataset-id dataset
              :table-id table}
           :source-format format
           :max-bad-records max-bad
           :source-uris (list gs-fn)
           :write-disposition "WRITE_TRUNCATE"}}})

(defun extract-csv-config (gcs-fn dataset table)
  #M{:configuration
     #M{
        :extract
        #M{
           :destination-uris (list gcs-fn)
           :source-table
           #M{
              :project-id *project*
              :dataset-id dataset
              :table-id table}}}})


(defun insert-job (config)
  (-> (post "/jobs" config)
    (mget* :job-reference :job-id)))

(defun insert-query-job (query dataset table)
  "Insert a query job, return the job id."
  (insert-job (query-job-config query dataset table)))

(defun insert-backup-load-job (gs-fn dataset table &key (max-bad 0))
  "Insert a backup load job, return the job id."
  (insert-job (load-job-config gs-fn dataset table :max-bad max-bad)))

(defun insert-json-load-job (gs-fn dataset table schema-fields)
  (insert-job (mset*
               (load-job-config gs-fn
                                dataset
                                table
                                :format "NEWLINE_DELIMITED_JSON")
               '(:configuration :load :schema)
               #M{:fields schema-fields})))

(defun insert-export-to-csv-job (gcs-fn dataset table)
  "Export the specified table to a CSV in GCS."
  (insert-job (extract-csv-config gcs-fn dataset table)))

(defun read-table-column-names (dataset table)
  (-> (http-get #?"/datasets/${dataset}/tables/${table}")
    (mget* :schema :fields)
    (rmapcar (lambda (f) (mget f :name)))))

(defun fetch-table-data (dataset table &key (n 5))
  ;; TODO: handle repeated values correctly
  (-> (http-get #?"/datasets/${dataset}/tables/${table}/data?maxResults=${n}")
    (mget :rows)
    (rmapcar (lambda (r) (mget r :f)))
    (rmapcar (lambda (col)
               (mapcar (lambda (val) (mget val :v))
                       col)))))


(pop-reader-exts)

(in-package :bq)

(enable-reader-exts)

(defparameter *default-dataset* "")

(define-condition bq-error (error)
  ((msg :initarg :msg :reader msg))
  (:report (lambda (condition stream)
             (format stream "BigQuery error: ~a" (msg condition)))))


(defun process-description (desc &key (comment-string "--"))
  (cl-ppcre:regex-replace-all #?/\n/ desc (<> (string #\newline) comment-string " ")))

(defmacro defquery (name &key (dataset *default-dataset*) table query
                              (description "") (tags nil) (title nil))
  "Define a query (function) that can be run via run-query-sync.

   required kwargs:
       table: the table into which the output will be written
       query: a string containing the query to run
   optional kwarg:
       dataset: the dataset in which the output table will be created.  Defaults
       to *default-dataset*.
"
  ;; TODO: this feels like a gratuitous macro.  Think more about the query
  ;; interface.
  `(defun ,name ()
     `(:dataset ,,dataset
       :table ,,table
       :query ,,query
       :description ,(process-description ,description)
       :tags ,(mapcar (lambda (tag) (<> "#" tag)) ,tags)
       :title ,(or ,title (symbol-name ',name))
       )))

(defun wait-on-job-completion (job-id &optional (poll-interval 1))
    "Wait for the specified job to complete.

Poll with exponential backoff.

   Args:
       job-id [string]: the id of the bigquery job to wait for
       poll-time [number]: the time in seconds (max 15) after which to
           poll for completion.
"
  (let* ((job-result (job-status job-id))
         (status (car job-result))
         (err (cadr job-result)))
    (when err
      (error 'bq-error :msg (format nil "~A" err)))
    (println #?"Waiting for job ${job-id}. Status is: ${status}.")
    (unless (equal status "DONE")
      (sleep poll-interval)
      (wait-on-job-completion job-id (min 15 (* 2 poll-interval))))))


(defun write-query-to-file (query query-file)
  (let* ((table (concatenate 'string
                             (mget query :dataset)
                             "."
                             (mget query :table)))
         (tagstring (format nil "~{~A~^ ~}" (mget query :tags)))

         (qcontents #?"
-- Title: ${(mget query :title)}
-- Description: ${(mget query :description)}
-- Tags: ${tagstring}
-- Result stored in: ${table}

${(mget query :query)}
"))
    (with-open-file (f query-file :direction :output :if-exists :supersede)
      (format f "~a" qcontents))))

(defun commit-and-push (repo tempdir query-file)
  (declare (ignore repo))
  #!"cd ${tempdir} && git add ${query-file} && git commit -n -m \"Saved query ${query-file} from cl-bq.\" && git push")

(defun delete-temp-clone (tempdir)
  (uiop:delete-directory-tree (make-pathname :directory `(:absolute ,tempdir)) :validate t))

(defun store-query-in-repo (&key repo path query)
  (let* ((tempdir (concatenate 'string
                              (namestring uiop:*temporary-directory*)
                              "temp-clone"))
         (query-file (concatenate 'string
                                  tempdir
                                  "/"
                                  path)))
    #!"git clone ${repo} ${tempdir}"
    (write-query-to-file query query-file)
    (commit-and-push repo tempdir path)
    (delete-temp-clone tempdir)))

(defun run-query-sync (qfun &key push-to-repo path export-to)
  "Run a query synchronously.

   Args:
       qfun: a query function as defined by defquery.
       push-to-repo: a git repository to push the query to
       path: the path in the repository in which to put the query
"
  (let* ((q-obj (funcall qfun))
         (q (regex-replace-all
             #?/\s+/
             (regex-replace-all #?/\s*--.*\n/ (mget q-obj :query) "")
             " ")))
    (println "Running query:")
    (println (mget q-obj :query))
    (println #?"--> ${(mget q-obj :dataset)}.${(mget q-obj :table)}\n\n\n")
    (wait-on-job-completion
         (insert-query-job q (mget q-obj :dataset) (mget q-obj :table)))
    (when push-to-repo
        (store-query-in-repo :repo push-to-repo
                             :path path
                             :query q-obj))
    (when export-to
      (println (format nil #?"Exporting to ${export-to}. Job id: ~A"
       (insert-export-to-csv-job export-to (mget q-obj :dataset) (mget q-obj :table)))))

    (terpri) (terpri) (terpri)))

(defun select (table-data &key (only nil) (except nil))
  ; unfortunately, the set operations don't guarantee post-operation ordering,
  ; so using loop...
  `((:headers . ,(loop for h in (mget table-data :headers)
                       when (member h only :test #'equal)
                         unless (member h except :test #'equal)
                           collect h))
    (:rows . ,(mapcar (lambda (r) (loop for h in (mget table-data :headers)
                                        for col in r
                                        when (member h only :test #'equal)
                                          unless (member h except :test #'equal)
                                            collect col))
                      (mget table-data :rows)))))

(defun read-table-data (table &key dataset n)
  (let ((headers (read-table-column-names dataset table))
        (data (fetch-table-data dataset table :n n)))
    (list :headers headers
          :rows data)))

(defun preview-table-data (table &key (dataset *default-dataset*) (n 5) (preprocess #'identity))
  (let* ((data (funcall preprocess (read-table-data table :dataset dataset :n n)))
         (tbl (make-table (mget data :headers))))
    (mapc (lambda (r) (add-row tbl r)) (mget data :rows))
    (display tbl)))

(defun run-in-order (qlist)
    (mapc #'run-query-sync qlist))

(pop-reader-exts)
