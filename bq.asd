(asdf:defsystem bq
  :description "Utility functions for interacting with the bigquery API."
  :version "0.0.1"
  :license "MIT"
  :defsystem-depends-on (:cl21)
  :class :cl21-system
  :depends-on
  (:cl-json
   :cjf-stdlib
   :drakma)
  :components
  ((:module "src"
    :components
    ((:file "bq")))))
