// bns project bns.go
package bns

const (
	leHeader = "II\x2A\x00" // Header for little-endian files.
	beHeader = "MM\x00\x2A" // Header for big-endian files.

	ifdLen = 10 // Length of an IFD entry in bytes.

)

const (
	ifdOffset        = 8
	Tiff_header_size = 160 // 0xA0
	xoffset          = 134
	yoffset          = 142
)

const (
	dtByte     = 1
	dtASCII    = 2
	dtShort    = 3
	dtLong     = 4
	dtRational = 5
)

// The length of one instance of each data type in bytes.
var lengths = [...]uint32{0, 1, 1, 2, 4, 8}

// Tags (see p. 28-41 of the spec).
const (
	tImageWidth                = 256
	tImageLength               = 257
	tBitsPerSample             = 258
	tCompression               = 259
	tPhotometricInterpretation = 262

	tStripOffsets    = 273
	tOrientation     = 274
	tSamplesPerPixel = 277
	tRowsPerStrip    = 278
	tStripByteCounts = 279

	tXResolution    = 282
	tYResolution    = 283
	tResolutionUnit = 296

	tPredictor    = 317
	tColorMap     = 320
	tExtraSamples = 338
	tSampleFormat = 339
)

const (
	cNone       = 1
	cCCITT      = 2
	cG3         = 3 // Group 3 Fax.
	cG4         = 4 // Group 4 Fax.
	cLZW        = 5
	cJPEGOld    = 6 // Superseded by cJPEG.
	cJPEG       = 7
	cDeflate    = 8 // zlib compression.
	cPackBits   = 32773
	cDeflateOld = 32946 // Superseded by cDeflate.
)

const (
	Bib      string = "BibliRangePageNumber"
	Abs      string = "AbsRangePageNumber"
	Cla      string = "ClaimsRangePageNumber"
	Dra      string = "DrawRangePageNumber"
	Amd      string = "AmdRangePageNumber"
	Des      string = "DescRangePageNumber"
	Srp      string = "SearchRepRangePageNumber"
	Dna      string = "DnaSequenceRangePageNumber"
	Aci      string = "ApplicantCitationsRangePageNumber"
	Cls      string = "Classification"
	Max_page int    = 1000 // maximum subpage
)
