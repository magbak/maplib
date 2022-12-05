from .maplib import Mapping
import logging
try:
    import rdflib
    from .functions import to_graph
except:
    logging.debug("RDFLib not found, install it to use the function to_graph")
