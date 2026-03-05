EXTENSION    = tpcds
EXTVERSION   = 1.0
DATA         = tpcds--$(EXTVERSION).sql
CONTROL      = tpcds.control

DSGEN_SRC    = DSGen-software-code-4.0.0
DSGEN_TOOLS  = $(DSGEN_SRC)/tools

# Detect pg_config
PG_CONFIG   ?= pg_config
SHAREDIR     = $(shell $(PG_CONFIG) --sharedir)
EXTDIR       = $(SHAREDIR)/extension
DSGEN_DEST   = $(EXTDIR)/tpcds_dsgen
QUERY_DEST   = $(DSGEN_DEST)/query_templates
CHART_DEST   = $(EXTDIR)/tpcds_chart

.PHONY: all build install install-segments uninstall clean

all: build

# Build dsdgen and dsqgen from TPC-DS v4 source
build:
	@if [ ! -d "$(DSGEN_TOOLS)" ]; then \
		echo "Error: $(DSGEN_TOOLS) not found."; \
		echo "The DSGen source directory should be part of this repository."; \
		exit 1; \
	fi
	$(MAKE) -C $(DSGEN_TOOLS) -f Makefile.suite OS=LINUX

# Install extension files + dsdgen/dsqgen binaries
install: build
	@mkdir -p $(EXTDIR) $(DSGEN_DEST)/tools $(QUERY_DEST) $(CHART_DEST)
	cp $(CONTROL) $(EXTDIR)/
	cp $(DATA) $(EXTDIR)/
	cp -n tpcds.conf $(EXTDIR)/
	cp $(DSGEN_TOOLS)/dsdgen $(DSGEN_DEST)/tools/
	cp $(DSGEN_TOOLS)/dsqgen $(DSGEN_DEST)/tools/
	cp $(DSGEN_TOOLS)/tpcds.idx $(DSGEN_DEST)/tools/
	cp $(DSGEN_SRC)/query_templates/*.tpl $(QUERY_DEST)/
	cp benchmarks/*.py $(CHART_DEST)/
	@echo "Installed $(EXTENSION) to $(EXTDIR)"
	@echo "Installed dsdgen/dsqgen to $(DSGEN_DEST)/tools/"
	@echo "Installed chart tools to $(CHART_DEST)/"

# Distribute dsdgen binary + tpcds.idx to all segment hosts
install-segments:
	@echo "Distributing dsdgen to segment hosts..."
	@for host in $$(psql -qAtc "SELECT DISTINCT hostname FROM gp_segment_configuration WHERE content >= 0 AND role = 'p'"); do \
		echo "  -> $$host"; \
		ssh $$host "mkdir -p ~/tpcds_tools" && \
		scp -q $(DSGEN_DEST)/tools/dsdgen $(DSGEN_DEST)/tools/tpcds.idx $$host:~/tpcds_tools/; \
	done
	@echo "Done."

uninstall:
	rm -f $(EXTDIR)/$(CONTROL)
	rm -f $(EXTDIR)/$(DATA)
	rm -f $(EXTDIR)/tpcds.conf
	rm -rf $(DSGEN_DEST)
	rm -rf $(CHART_DEST)
	@echo "Uninstalled $(EXTENSION)"

clean:
	@if [ -d "$(DSGEN_TOOLS)" ]; then \
		$(MAKE) -C $(DSGEN_TOOLS) -f Makefile.suite clean 2>/dev/null || true; \
	fi
