"""
A converter from RDF/NTriples format into Postgres-compliant CSV format. 

This tool is a fork of the CSV converter tool from the U{BigQuery for Linked Data <http://code.google.com/p/bigquery-linkeddata/>} effort.

@author: George London (tw: @rogueleaderr)
@since: 2012-07-11
@status: good
"""

import sys, csv, os, StringIO, logging

import rdflib

csv.register_dialect('rdf_csv', delimiter='|', doublequote=False, quoting=csv.QUOTE_NONE, escapechar="\\")
logging.basicConfig(format='%(asctime)-6s: %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

"""
The NTriple/CSV converter class.
"""
class NTriple2CSV:
	def __init__(self, fname):
		"""
		Initialises the converter. If a .nt file is given, the converter uses this as an input to create a .csv file. 
		However, if a directory is specified, then all .nt files in this directory are converted to .csv files.
		
		@param fname: The input NTriples file (or directory with NTriples files).
		"""
		if fname:
			self.ifname = fname
			self.dodir = os.path.isdir(fname) # flag to remember if we're working on a directory or single file

	def create_outfilename(self, infilename):
		(workpath, outfilename) = os.path.split(infilename)
		(filename, ext) = os.path.splitext(outfilename) 
		return os.path.join(workpath, filename + '.csv')

	def convertstr(self, ntstr):
		"""
		Takes a string in RDF/U{NTriples format <http://www.w3.org/TR/rdf-testcases/#ntriples>} and turns 
		it into a U{BigQuery-compliant <http://code.google.com/apis/bigquery/docs/uploading.html#createtabledata>} 
		CSV string.
		"""
		#: prep output so that the CSV is compliant with Postgres' format
		outbuffer = StringIO.StringIO()
		out = csv.writer(outbuffer, dialect="rdf_csv")
		#: read RDF NTriples content line by line ...
		try:
			rdf_lines = csv.reader(ntstr.split('\n'))
			self.parse_and_write(rdf_lines, out)
		except csv.Error, e:
		    sys.exit('line %d: %s' %(rdf_lines.line_num, e))
		retv = outbuffer.getvalue()
		outbuffer.close()
		return retv		
	
	def convertfile(self, ifile):
		"""
		Takes an RDF file in U{NTriples format <http://www.w3.org/TR/rdf-testcases/#ntriples>} and turns it into a U{BigQuery-compliant <http://code.google.com/apis/bigquery/docs/uploading.html#createtabledata>}  CSV file.
		"""
		def chunk_write(): # inner function can access local scope
			while True:
				chunk = []
				for i in range(0, chunk_len):
					line = in_stream.readline()
					if not line:
						return
					chunk.append(line)
				self.parse_and_write(chunk, out_stream)
				
		ofile = self.create_outfilename(ifile)
		#: prep output so that the CSV is compliant with BigQuery's format
		out_stream = open(ofile, 'wb')
		in_stream = open(ifile, 'rb')
		#: read RDF NTriples content in chunks ...
		chunk_len = 10000
		chunk_write()
		print 'Done converting ' + ifile + ' to ' + ofile
		
		
		
	def parse_and_write(self, rdf_lines, out):
		bqcsv = csv.writer(out, dialect="rdf_csv")
		g = rdflib.Graph()
		line_no = 0
		for line in rdf_lines:
			try:
				triples = g.parse(data=line, format="nt")
				#: ... and create CSV in Postgres CSV format
				for triple in triples:
					try:
						s = str(triple[0])
						p = str(triple[1])
						o = str(triple[2])
						bqcsv.writerow((line_no, s, p, o))
						line_no += 1
					except Exception, e:
						logger.warning("Problem with %s, error %s" % (triple, e))
			except Exception, e:
				logger.warning("Could not parse %s, error %s" % (line, e))

	def convert(self):
		"""
		Depending on the initialisation, converts a single NTriple file or an entire directory with NTriple files to CSV.
		"""
		if self.dodir == True: # process *.nt in directory
			input_files = [f for f in os.listdir(self.ifname) if os.path.isfile(os.path.join(self.ifname, f)) and f.endswith('.nt')]
		 	for f in input_files:
				self.convertfile(os.path.join(self.ifname, f))
		else: # process a single .nt file
			self.convertfile(self.ifname)

def main():
	if sys.argv[1] == '-h':
		print ' Takes either a single RDF file in NTriple format or a directory with NTriple files'
		print ' along with a target graph URI as input (if input is a directory then all the files will'
		print ' go into the same target graph) and creates Postgres-compliant CSV file(s) with the same'
		print ' name as the input file(s) in the target graph.'
		print ' --------------------------------------------------------------------------------------------------'
		print ' EXAMPLE: $python tools/nt2csv.py test/mhausenblas-foaf.nt http://example.org/mhausenblas'
	elif sys.argv[1] == '-t':
		nt2csv = NTriple2CSV(None, None)
		print nt2csv.convertstr('<http://example.org/#this> <http://example.org/p1> "abc english"@en .\n<http://example.org/#this> <http://example.org/p2> "abc @de"@en .', 'http://example.org/default')
	else:
		infile = sys.argv[1]
		nt2csv = NTriple2CSV(infile)
		nt2csv.convert()
		
if __name__ == '__main__':
	main()
