(asdf:defsystem bq
  :description "Utility functions for interacting with the bigquery API."
  :version "0.0.1"
  :license "MIT"
  :depends-on
  (:cl-json
   :cl-annot
   :cl-ppcre
   :cl-interpol
   :col
   :drakma
   :cl-ascii-table
   :split-sequence)
  :components
  ((:module "src"
    :components
    ((:file "bq")))))
