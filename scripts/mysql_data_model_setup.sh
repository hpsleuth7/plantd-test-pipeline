mysql -ulocal -p123456 --database="pipeline" < pipeline.product.ddl && \
mysql -ulocal -p123456 --database="pipeline" < pipeline.supplier.ddl  && \
mysql -ulocal -p123456 --database="pipeline" < pipeline.warehouse.ddl