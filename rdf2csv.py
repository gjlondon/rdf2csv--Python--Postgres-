"""
A converter from RDF/NTriples format into BigQuery-compliant CSV format. 

This tool is part of the U{BigQuery for Linked Data <http://code.google.com/p/bigquery-linkeddata/>} effort.

@author: Michael Hausenblas, http://sw-app.org/mic.xhtml#i
@since: 2010-12-12
@status: supports now quintuple scheme and in-memory (string) conversion
"""

import sys, csv, os, StringIO

"""
The NTriple/CSV converter class.
"""
class NTriple2CSV:
	def __init__(self, fname, tgraph):
		"""
		Initialises the converter. If a .nt file is given, the converter uses this as an input to create a .csv file. 
		However, if a directory is specified, then all .nt files in this directory are converted to .csv files.
		
		@param fname: The input NTriples file (or directory with NTriples files).
		@param tgraph: The target graph URI.
		"""
		if fname:
			self.ifname = fname
			self.dodir = os.path.isdir(fname) # flag to remember if we're working on a directory or single file
		if tgraph:
			self.tgraph = tgraph

	def create_outfilename(self, infilename):
		(workpath, outfilename) = os.path.split(infilename)
		(filename, ext) = os.path.splitext(outfilename) 
		return os.path.join(workpath, filename + '.csv')
		
	def strip_anglebr(self, text):
		"""
		Strips angle barckets from string, that is <...> into ...
		@return: the string with '<'...'>' removed
		"""
		if text.startswith('<'):
			text = text.strip('<')
		if text.endswith('>'):
			text = text.strip('>') 
		return text

	def split_object(self, text):
		if text.find('^^') >= 0:
			return (self.strip_datatype(text), self.extract_datatype(text))
		elif text.find('@') >= 0:
			return (self.strip_langtag(text), self.extract_langtag(text))
		else:
			return (text, "")
			
	def strip_datatype(self, text):
		"""
		Removes NTriple datatype, that is, it turns '...'^^<http://www.w3.org/2001/XMLSchema#string> into '...'
		@return: the string with '^^<...>' removed
		"""
		return text.split('^^')[0]

	def strip_langtag(self, text):
		"""
		Removes NTriple language tag, that is, it turns '...'@de  into '...'
		@return: the string with '@..' removed
		"""
		return text[:text.rfind('@')]

	def extract_datatype(self, text):
		"""
		Extracts the NTriple datatype, that is, it turns '...'^^<http://www.w3.org/2001/XMLSchema#string> into 'http://www.w3.org/2001/XMLSchema#string'
		@return: the NTriple datatype as a string
		"""
		return self.strip_anglebr(text.split('^^')[1])

	def extract_langtag(self, text):
		"""
		Extracts the NTriple language tag, that is, it turns '...'@de into 'de'
		@return: the NTriple language tag as a string
		"""
		text = text[text.rfind('@'):]
		return text.split('@')[1]

	def convertstr(self, ntstr, tg):
		"""
		Takes a string in RDF/U{NTriples format <http://www.w3.org/TR/rdf-testcases/#ntriples>} and turns it into a U{BigQuery-compliant <http://code.google.com/apis/bigquery/docs/uploading.html#createtabledata>}  CSV string.
		"""
		#: prep output so that the CSV is compliant with BigQuery's format
		outbuffer = StringIO.StringIO()
		bqcsv = csv.writer(outbuffer, delimiter=',', quoting=csv.QUOTE_ALL)

		#: read RDF NTriples content line by line ...
		try:
			rdftriples = csv.reader(ntstr.split('\n'), delimiter=' ', quoting=csv.QUOTE_ALL)
			for triple in rdftriples:
				#: ... and create CSV in BigQuery CSV format
				s = self.strip_anglebr(triple[0])
				p = self.strip_anglebr(triple[1])
				#print triple[2]
				(o, o_type) = self.split_object(self.strip_anglebr(triple[2]))
				bqcsv.writerow([tg, s, p, o, o_type])
		except csv.Error, e:
		    sys.exit('line %d: %s' %(rdftriples.line_num, e))
		retv = outbuffer.getvalue()
		outbuffer.close()
		return retv	

	def convertfile(self, ifile, tg):
		"""
		Takes an RDF file in U{NTriples format <http://www.w3.org/TR/rdf-testcases/#ntriples>} and turns it into a U{BigQuery-compliant <http://code.google.com/apis/bigquery/docs/uploading.html#createtabledata>}  CSV file.
		"""
		ofile = self.create_outfilename(ifile)
		
		#: prep output so that the CSV is compliant with BigQuery's format
		bqcsv = csv.writer(open(ofile, 'wb'), delimiter='|', quoting=csv.QUOTE_ALL)

		#: read RDF NTriples content line by line ...
		i = 0
		try:
			rdftriples = csv.reader(open(ifile, 'rb'), delimiter=' ', quoting=csv.QUOTE_ALL)
			for triple in rdftriples:
				#: ... and create CSV in BigQuery CSV format
				s = self.strip_anglebr(triple[0])
				p = self.strip_anglebr(triple[1])
				#print triple[2]
				(o, o_type) = self.split_object(self.strip_anglebr(triple[2]))
				bqcsv.writerow([i, s, p, o, o_type])
				i += 1
		except csv.Error, e:
		    sys.exit('file %s, line %d: %s' % (ofile, rdftriples.line_num, e))

		print 'Done converting ' + ifile + ' to ' + ofile

	def convert(self):
		"""
		Depending on the initialisation, converts a single NTriple file or an entire directory with NTriple files to CSV.
		"""
		if self.dodir == True: # process *.nt in directory
			input_files = [f for f in os.listdir(self.ifname) if os.path.isfile(os.path.join(self.ifname, f)) and f.endswith('.nt')]
		 	for f in input_files:
				self.convertfile(os.path.join(self.ifname, f), self.tgraph)
		else: # process a single .nt file
			self.convertfile(self.ifname, self.tgraph)

def main():
	if sys.argv[1] == '-h':
		print ' Takes either a single RDF file in NTriple format or a directory with NTriple files'
		print ' along with a target graph URI as input (if input is a directory then all the files will'
		print ' go into the same target graph) and creates BigQuery-compliant CSV file(s) with the same'
		print ' name as the input file(s) in the target graph.'
		print ' --------------------------------------------------------------------------------------------------'
		print ' EXAMPLE: $python tools/nt2csv.py test/mhausenblas-foaf.nt http://example.org/mhausenblas'
	elif sys.argv[1] == '-t':
		nt2csv = NTriple2CSV(None, None)
		print nt2csv.convertstr('<http://example.org/#this> <http://example.org/p1> "abc english"@en .\n<http://example.org/#this> <http://example.org/p2> "abc @de"@en .', 'http://example.org/default')
	else:
		infile = sys.argv[1]
		targetgraph = sys.argv[2]
		nt2csv = NTriple2CSV(infile, targetgraph)
		nt2csv.convert()
		
if __name__ == '__main__':
	main()
