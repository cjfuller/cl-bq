(asdf:defsystem bq
  :description "Utility functions for interacting with the bigquery API."
  :version "0.0.1"
  :license "MIT"
  :depends-on
  (:cl-json
   :cl-ppcre
   :cl-interpol
   :inferior-shell
   :cjf-stdlib
   :drakma)
  :components
  ((:module "src"
    :components
    ((:file "bq")))))
