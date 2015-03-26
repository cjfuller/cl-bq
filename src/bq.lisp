(in-package :cl21-user)

(defpackage :bq
  (:use :cl21
        :cl21.re
        :cl-json
        :cjf-stdlib)
  (:export :format-json-req
           :get-job-status
           :run-query-sync
           :backoff-time
           :wait-job
           :run-in-order
           :*project*
           :*output-dataset*))

(in-package :bq)

(defparameter *credentials*
  (decode-json-from-source
   (pathname #"${(namestring (user-homedir-pathname))}/.config/gcloud/credentials")))

(defparameter *token* (mget* (car (mget *credentials* :data)) :credential :access--token))

(defparameter *project* "")

; TODO: this shouldn't be a parameter here, but passed along with the query.
(defparameter *output-dataset* "")

(defun jobs-url ()
  #"https://www.googleapis.com/bigquery/v2/projects/${*project*}/jobs")

; Don't print all the headers with the bearer token to stdout.
(setf drakma:*header-stream* (make-broadcast-stream))

(defun format-json-req (q dest)
  "Format a json string to use as the request body for the query API call."
  (encode-json-plist-to-string
   `(:configuration
        ((:query .
            ((:allow-large-results . t)
             (:destination-table .
               ((:dataset-id . ,*output-dataset*)
                (:project-id . ,*project*)
                (:table-id . ,dest)))
             (:query . ,q)
             (:write-disposition . "WRITE_TRUNCATE")))))))

(defun get-job-status (job-id)
  (let* ((response
           (decode-json
            (drakma:http-request #"${(jobs-url)}/${job-id}"
                                 :method :get
                                 :want-stream t
                                 :additional-headers `((:|Authorization| .
                                                         ,#"Bearer ${*token*}")))))
         (status (mget* response :status :state))
         (err-msg (mget* response :status :error-result)))
    (list status err-msg)))

(defun run-query-sync (qfun)
  (let* ((q-obj (funcall qfun))
         (q (re-replace #/\s+/g
                        (re-replace #/\s*--.*\n/g (getf q-obj :query) "")
                        " "))
         (dest (getf q-obj :destination-table))
         (json-body (format-json-req q dest)))
    (println "Running query: ")
    (println q)
    (println #"--> ${*output-dataset*}.${dest}\n\n\n")
    (let* ((response (decode-json (drakma:http-request (jobs-url)
                         :method :post
                         :want-stream t
                         :additional-headers `((:|Authorization| . ,#"Bearer ${*token*}"))
                         :content-type "application/json; charset=UTF-8"
                         :content json-body)))
           (job-id (mget* response :job-reference :job-id)))
      (if (mget response :error)
          (error (format nil "~a" response))
          (wait-job job-id 1))))
  (princ "\n\n\n"))

(defun backoff-time (init-time)
  (let ((new-time (* 2 init-time)))
    (if (> new-time 15)
        15
        new-time)))

(defun wait-job (job-id poll-time)
  (destructuring-bind (status err-msg) (get-job-status job-id)
    (println #"Waiting for job ${job-id}.  Status is: ${status}.")
    (cond
      ((equal status "DONE")
       (if err-msg
           (error (format nil "~a" err-msg))
           nil))
      (t (progn
           (sleep poll-time)
           (wait-job job-id (backoff-time poll-time)))))))

(defun run-in-order (qlist)
    (map #'run-query-sync qlist))
