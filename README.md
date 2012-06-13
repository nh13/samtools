## This is *NOT* the official repository of SAMtools.  
The official SAMtools repository can be found at: http://samtools.sourceforge.net/ 

## Do you like what you see here?
There are a number of features found here that are not in the samtools repository.  If you find them 
useful, consider writing to the SAMtools mailing lists to encourage their inclusion in the main, 
support repository: samtools-help@lists.sourceforge.net.

## Major fixes:
	- SAM Header API
		- Clear and easy to use to query and modify the SAM Header
	- SAM/BAM reader is now multi-threaded, significantly speeding up reading and writing of BAM files.
	    - To remove support for parallel reading and writing of files, modify the Makefile: 
		remove "-D_PBGZF_USE"
		- Use the "-n" option _before_ a command (i.e. "samtools -n 8 view ...") to specify the 
		number of threads
		- BZ2 support is added, as it is also a block-level compression technique.
    - added "samtools qa" command, to compute the mean and median coverage, as well a histogram 
    from 1 to N (defined by param) containing the number of bases covered a maximum of 1X, 2X...NX. 
    Furthermore, "other" information is also available in the output file, namely:
        - Total number of reads
        - Total number of duplicates found and ignored (duplicates are "found" based on the sam flag 
        and are ignored in the counting of coverage)
        - Percentage of unmapped reads
        - Percentage of zero quality mappings
        - Number of proper paired reads (based on sam flag of proper pairs)
        - Percentage of proper pairs.e

## Minor fixes:
    - Check the write filehandle after opening for write.
    - allow for user defined [lowercase] tags in header elements.
    - allow the maximum memory for "samtools sort" to be specified with units.
    - adjust for leading hard clip on colorspace reads.
    - catches and reports an invalid BAM header, instead of segfaulting later on.
    - fixes a small underflow/overflow bug in integer parsing.
    - checks for a lowerbound in text entry box to avoid segfault in tview.
