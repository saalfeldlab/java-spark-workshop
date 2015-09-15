/**
 * 
 */
package org.janelia.sort.tsp;

import ij.ImagePlus;
import ij.io.Opener;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.TreeMap;

import org.apache.commons.io.IOUtils;
import org.janelia.sort.tsp.conversion.DataToStringFullMatrixTSPLIB;
import org.janelia.sort.tsp.conversion.DataToStringInterface;
import org.janelia.sort.tsp.conversion.SimilarityToDistanceInterface;
import org.janelia.sort.tsp.conversion.SimilarityToDistanceSigmoid;

import net.imglib2.Cursor;
import net.imglib2.RandomAccess;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.ImagePlusAdapter;
import net.imglib2.img.Img;
import net.imglib2.img.ImgFactory;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.img.imageplus.FloatImagePlus;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.integer.IntType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;

/**
 * @author Philipp Hanslovsky <hanslovskyp@janelia.hhmi.org>
 *
 * Collection of convenience functions for transferring a similarity matrix into a
 * traveling salesman problem (TSP) and rewriting the matrix according to the result.
 * 
 */
public class TSP {

	public static void main(String[] args) {

		try {

			if (args.length != 2) {
				throw new IllegalArgumentException("USAGE: java " + TSP.class + " <concorde path> <similarity matrix image>");
			}

			final File concordeFile = new File(args[0]).getCanonicalFile();
			final File matrixFile = new File(args[1]).getCanonicalFile();

			if (! concordeFile.canExecute()) {
				throw new IllegalArgumentException("cannot excecute " + concordeFile);
			}

			if (! matrixFile.canRead()) {
				throw new IllegalArgumentException("cannot find " + matrixFile);
			}

			final Opener opener = new Opener();
			final ImagePlus matrixImagePlus = opener.openImage(matrixFile.getAbsolutePath());
			final FloatImagePlus<FloatType> wrappedMatrix = ImagePlusAdapter.wrapFloat(matrixImagePlus);

			logInfo("loaded " + matrixFile.getAbsolutePath() + " " + matrixImagePlus);

			TSP.generateResultFiles(wrappedMatrix,
									concordeFile.getAbsolutePath(),
									matrixFile.getParent());
		} catch (Throwable t) {
			t.printStackTrace();
		}

	}

	public static void generateResultFiles(final FloatImagePlus<FloatType> matrix,
										   final String concordeExecutablePath,
										   final String targetDirectoryPath)
			throws IOException, InterruptedException {

		logInfo("generateResultFiles: entry");

		final File concordeExeFile = new File(concordeExecutablePath);
		if (! concordeExeFile.canExecute()) {
			throw new IOException("concorde executable " + concordeExeFile.getAbsolutePath() + " cannot be run");
		}

		final File targetDirectory = new File(targetDirectoryPath);
		if (! targetDirectory.exists()) {
			if (! targetDirectory.mkdirs()) {
				throw new IOException("failed to create " + targetDirectory.getAbsolutePath());
			}
		}

		logInfo("generateResultFiles: cleanMatrix ...");

//            ImageJFunctions.show( matrix, "original matrix" );
		final ArrayList<Long> remove = new ArrayList<>();
		final ArrayList<Long> keep = new ArrayList<>();
		final boolean[] sectionStatus = new boolean[ (int) matrix.dimension( 0 ) ];
		final ArrayList<ArrayList<Long>> badSuccessors = new ArrayList<>();
		final RandomAccessibleInterval<FloatType> cleanMatrix = TSP.cleanMatrix(matrix,
																				remove,
																				keep,
																				badSuccessors,
																				sectionStatus);
		final int n = (int) cleanMatrix.dimension( 0 );

		logInfo("generateResultFiles: convertMatrix ...");

		final String distances = TSP.convertMatrix(cleanMatrix,
												   new DataToStringFullMatrixTSPLIB(),
												   new SimilarityToDistanceSigmoid(10000.0, 0.0, 1000000));

//            ImageJFunctions.show( cleanMatrix, "matrix without bad sections" );

		final File concordeInputFile = new File(targetDirectory, "concorde.in");
		Files.write(Paths.get(concordeInputFile.getAbsolutePath()), distances.getBytes());

		final File concordeOutputFile = new File(targetDirectory, "concorde.out");


		logInfo("generateResultFiles: run concorde ...");

		final String concordeDeleteFilenameArgument = "";
		final Process proc = TSP.runConcordeTSPSolver(concordeExecutablePath,
													  concordeInputFile.getAbsolutePath(),
													  concordeOutputFile.getAbsolutePath(),
													  concordeDeleteFilenameArgument);
		proc.waitFor();

		if (proc.exitValue() != 0) {
			final String errorMessage = IOUtils.toString(proc.getErrorStream());
			throw new IOException("concorde failed with exit code " + proc.exitValue() +
								  " and error message:\n" + errorMessage);
		}

		logInfo("generateResultFiles: load concorde results ...");

		final int[] result = TSP.tspResultToArrayRespectDummyNode(concordeOutputFile.getAbsolutePath(), n);

//        final RandomAccessibleInterval<FloatType> sortedMatrix = TSP.rearrangeMatrix(cleanMatrix, result);
//            ImageJFunctions.show( sortedMatrix, "sorted matrix" );

		logInfo("generateResultFiles: addInvalidSections ...");

		final int[] resultWithBadSections = TSP.addInvalidSections(result,
																   badSuccessors,
																   sectionStatus,
																   (int) matrix.dimension(0));

//        final RandomAccessibleInterval<FloatType> sortedMatrixWithBadSections =
//                TSP.rearrangeMatrix(matrix, resultWithBadSections);
//            ImageJFunctions.show( sortedMatrixWithBadSections, "sorted matrix including bad sections" );
//            IJ.log( "Section order (no bad sections): " + Arrays.toString( result ) );
//            IJ.log( "Section order:                   " + Arrays.toString( resultWithBadSections ) );


		logInfo("generateResultFiles: aggregate results ...");

		final StringBuilder forwardMappings = new StringBuilder(resultWithBadSections.length * 20);
		final StringBuilder backwardMappings = new StringBuilder(resultWithBadSections.length * 20);
		final StringBuilder removedSections = new StringBuilder(remove.size() * 10);
		for (int i = 0; i < resultWithBadSections.length; ++i ) {
			final int currVal = resultWithBadSections[i];
			final int currStatus = sectionStatus[currVal] ? 1 : 0;
			forwardMappings.append(currVal).append(',').append(i).append(',').append(currStatus).append('\n');
			backwardMappings.append(i).append(',').append(currVal).append(',').append(currStatus).append('\n');
		}

		Files.write(Paths.get(targetDirectoryPath, "forward-mapping.csv"), forwardMappings.toString().getBytes());
		Files.write(Paths.get(targetDirectoryPath, "backward-mapping.csv"), backwardMappings.toString().getBytes());

		for (final Long removedSection : remove) {
			removedSections.append(removedSection).append('\n');
		}

		Files.write(Paths.get(targetDirectoryPath, "removed-sections.txt"), removedSections.toString().getBytes());

		logInfo("generateResultFiles: exit");

	}

	/**
	 * @param matrix 2D {@link RandomAccessibleInterval} containing the similarity matrix
	 * @param converter {@link DataToStringInterface} determining the rule for converting data to string
	 * @param similarityToDistance {@link SimilarityToDistanceInterface} for converting similarities into distances
	 * @return {@link String} that contains all the information necessary for TSP solver
	 */
	public static < T extends RealType<T> & NativeType< T > > String convertMatrix( 
			final RandomAccessibleInterval< T > matrix,
			final DataToStringInterface converter,
			final SimilarityToDistanceInterface similarityToDistance ) {
		
		assert matrix.numDimensions() == 2: "Need two-dimensional matrix";
		assert matrix.dimension( 0 ) == matrix.dimension( 1 ): "Matrix needs to be quadratic";
		
		final long n = matrix.dimension( 0 );
		
		// initialize converter with number of nodes
		converter.initialize( (int) n );
		
		// for each row convert each column and 
		for ( int i = 0; i < n; ++i ) {
			final IntervalView<T> row = Views.hyperSlice( matrix, 0, i );
			final Cursor<T> r         = Views.flatIterable( row ).cursor();
			for ( int j = 0; r.hasNext(); ++j ) {
				converter.addSimilarity( i, j, similarityToDistance.convert( r.next().getRealDouble() ) );
			}
			// add zero distance dummy to transfer TSP into sorting problem
			converter.addDummy( i, 0.0 );
		}
		
		// close out converter and obtain string
		return converter.close();
	}
	
	
	
	/**
	 * Clean similarity matrix from "empty" sections 
	 * @param matrix input matrix
	 * @param removedIndices output parameter for removed row/column indices
	 * @param keptIndices output parameter for remaining row/column indices
	 * @param sectionStatus output parameter that keeps boolean true for good sections, false otherwise
	 * @return matrix without holes. If the original matrix does not have any holes, return original matrix.
	 */
	public static < T extends RealType< T > & NativeType< T > > RandomAccessibleInterval< T > cleanMatrix(
			final RandomAccessibleInterval< T > matrix,
			final ArrayList< Long > removedIndices,
			final ArrayList< Long > keptIndices,
			final ArrayList< ArrayList< Long > > badSuccessors,
			final boolean[] sectionStatus
			)
	{
		return cleanMatrix(matrix, removedIndices, keptIndices, badSuccessors, sectionStatus, new ArrayImgFactory<T>());
	}
	
	/**
	 * Clean similarity matrix from "empty" sections 
	 * @param matrix input matrix
	 * @param removedIndices output parameter for removed row/column indices
	 * @param keptIndices output parameter for remaining row/column indices
	 * @param sectionStatus output parameter that keeps boolean true for good sections, false otherwise
	 * @param factory ImgFactory used for creating output matrix.
	 * @return matrix without holes. If the original matrix does not have any holes, return original matrix.
	 */
	public static < T extends RealType< T > & NativeType< T > > RandomAccessibleInterval< T > cleanMatrix(
			final RandomAccessibleInterval< T > matrix,
			final ArrayList< Long > removedIndices,
			final ArrayList< Long > keptIndices,
			final ArrayList< ArrayList< Long > > badSuccessors,
			final boolean[] sectionStatus,
			final ImgFactory< T > factory
			)
	{
		assert matrix.numDimensions() == 2: "Need two-dimensional matrix";
		assert matrix.dimension( 0 ) == matrix.dimension( 1 ): "Matrix needs to be quadratic";
		
		final long n = matrix.dimension( 0 );
		
		removedIndices.clear(); // maybe do not call clear?
		keptIndices.clear(); // maybe do not call clear?
		// loop over all rows and
		// if row is entirely NaN/0.0:
		//    store index for removal
		// else:
		//    store index for result matrix
		ArrayList<Long> bs = new ArrayList< Long >();
		for ( long i = 0; i < n; ++i ) {
			final Cursor<T> row = Views.flatIterable( Views.hyperSlice( matrix, 0, i ) ).cursor();
			boolean isBad = true;
			while ( row.hasNext() ) {
				final double val = row.next().getRealDouble();
				// as soon as a non-zero value is also not NaN, break and store index
				if ( !Double.isNaN( val ) && val != 0.0 ) {
					bs = new ArrayList< Long >();
					badSuccessors.add( bs );
					isBad = false;
					keptIndices.add( i );
					sectionStatus[ (int)i ] = true;
					break;
				}
			}
			// if only zero or NaN values, remove index
			if ( isBad ) {
				removedIndices.add( i );
				sectionStatus[ (int)i ] = false;
				bs.add( i );
			}
		}
		
		
		// if nothing needs to be removed, return original matrix, else create matrix w/o removed indices
		if ( removedIndices.size() > 0 ) {
			final long[] newDimension = new long[] { n - removedIndices.size(), n - removedIndices.size() };
			final Img<T> result     = factory.create( newDimension, matrix.randomAccess().get() );
			final Cursor<T> c       = Views.flatIterable( result ).cursor();
			final RandomAccess<T> r = matrix.randomAccess();
			while( c.hasNext() ) {
				c.fwd();
				// get coordinates of target images and look up the corresponding indices of the original matrix
				// in the list of saved coordinates (keptIndices)
				final Long xTrans = keptIndices.get( c.getIntPosition( 0 ) );
				final Long yTrans = keptIndices.get( c.getIntPosition( 1 ) );
				r.setPosition( xTrans, 0 );
				r.setPosition( yTrans, 1 );
				// write value of the corresponding position within the old matrix into the current position
				// of the result matrix
				c.get().set( r.get() );
			}
			return result;
		}
		else {
			return matrix;
		}
		
	}
	
	
	public static void getMappings( final boolean[] sectionStatus, final TreeMap< Integer, Integer > fullToDeleted, final TreeMap< Integer, Integer > deletedToFull ) {
		fullToDeleted.clear();
		deletedToFull.clear();
		for ( int fullIndex = 0, deletedIndex = 0; fullIndex < sectionStatus.length; ++fullIndex ) {
			if ( !sectionStatus[ fullIndex ] )
				continue;
			fullToDeleted.put( fullIndex, deletedIndex );
			deletedToFull.put( deletedIndex, fullIndex );
			++deletedIndex;
		}
	}
	
	
	public static int[] addInvalidSections( final int[] solution, final ArrayList< ArrayList< Long > > badSuccessors, final boolean[] sectionStatus, final int length ) {
		final int[] result = new int[ length ];
		addInvalidSections(solution, badSuccessors, sectionStatus, result);
		return result;
	}
	
	
	public static void addInvalidSections( final int[] solution, final ArrayList< ArrayList< Long > > badSuccessors, final boolean[] sectionStatus, final int[] result ) {
		
		final TreeMap< Integer, Integer > fullToDeleted = new TreeMap< Integer, Integer >(); 
		final TreeMap< Integer, Integer > deletedToFull = new TreeMap< Integer, Integer >();
		getMappings( sectionStatus, fullToDeleted, deletedToFull);

		int solutionIndex = 0;
		int resultIndex   = 0;
		for( ; solutionIndex < solution.length; ++solutionIndex, ++resultIndex ) {
			final int currIndex = solution[ solutionIndex ];
			final ArrayList<Long> bs = badSuccessors.get( currIndex );
			final int fullIndex  = deletedToFull.get( currIndex );
			result[ resultIndex ] = fullIndex;
			for ( int k = 0; k < bs.size(); ++k ) {
				++resultIndex;
				result[ resultIndex ] = fullIndex + k + 1;
			}
		}
		
	}
	
	
	/**
	 * Make external call to concorde solver, assuming that the concorde executable path is just "concorde"
	 * @param inputFileName file containing tsp in full matrix tsplib format
	 * @param outputFileName output file for concorde, will be overwritten if existing
	 * @throws IOException
	 */
	public static Process runConcordeTSPSolverWithDefaultConcorde( 
			final String inputFileName, 
			final String outputFileName
			) throws IOException {
		return runConcordeTSPSolverWithDefaultConcorde( inputFileName, outputFileName, "" );
	}
	
	
	/**
	 * Make external call to concorde solver, assuming that the concorde executable path is just "concorde"
	 * @param inputFileName file containing tsp in full matrix tsplib format
	 * @param outputFileName output file for concorde, will be overwritten if existing
	 * @param additionalArgument additional arguments for concorde, i.e. seed
	 * @throws IOException
	 */
	public static Process runConcordeTSPSolverWithDefaultConcorde( 
			final String inputFileName, 
			final String outputFileName, 
			final String additionalArgument 
			) throws IOException {
		return runConcordeTSPSolver("concorde", inputFileName, outputFileName, additionalArgument);
	}
	
	
	/**
	 * Make external call to concorde solver
	 * @param concordeExecutablePath path to concorde executable
	 * @param inputFileName file containing tsp in full matrix tsplib format
	 * @param outputFileName output file for concorde, will be overwritten if existing
	 * @throws IOException
	 */
	public static Process runConcordeTSPSolver( 
			final String concordeExecutablePath, 
			final String inputFileName, 
			final String outputFileName
			) throws IOException {
		return runConcordeTSPSolver(concordeExecutablePath, inputFileName, outputFileName, "");
	}
	
	
	/**
	 * Make external call to concorde solver
	 * @param concordeExecutablePath path to concorde executable
	 * @param inputFileName file containing tsp in full matrix tsplib format
	 * @param outputFileName output file for concorde, will be overwritten if existing
	 * @param additionalArgument additional arguments for concorde, i.e. seed
	 * @throws IOException
	 */
	public static Process runConcordeTSPSolver( 
			final String concordeExecutablePath, 
			final String inputFileName, 
			final String outputFileName, 
			final String additionalArgument
			) throws IOException {
		final String command = String.format("%s %s -o %s %s", concordeExecutablePath, additionalArgument, outputFileName, inputFileName);
		logInfo("running " + command);
		final ProcessBuilder processBuilder;
		if ((additionalArgument == null) || (additionalArgument.trim().length() == 0)) {
			processBuilder = new ProcessBuilder().inheritIO().command(concordeExecutablePath, "-o", outputFileName, inputFileName);
		} else {
			processBuilder = new ProcessBuilder().inheritIO().command(concordeExecutablePath, additionalArgument, "-o", outputFileName, inputFileName);
		}
		return processBuilder.start();
//		final Process proc = Runtime.getRuntime().exec( command );
//		return proc;
	}
	
	
	/**
	 * Translate concorde result into array that associates the array index with sections in the original matrix
	 * @param tspResultFileName path to the output of the concorde output
	 * @param n number of sections
	 * @return int[] that associates with each index (of the target matrix) the reference section from the original matrix, return value is null in case of exception
	 */
	public static int[] tspResultToArray( final String tspResultFileName, final int n ) {
		return tspResultToArray(tspResultFileName, n, Charset.defaultCharset() );
	}
	
	
	/**
	 * Translate concorde result into array that associates the array index with sections in the original matrix
	 * @param tspResultFileName path to the output of the concorde output
	 * @param n number of sections
	 * @param cs charset for text file
	 * @return int[] that associates with each index (of the target matrix) the reference section from the original matrix, return value is null in case of exception
	 */
	public static int[] tspResultToArray( final String tspResultFileName, final int n, final Charset cs ) {
		return tspResultToArray( tspResultFileName, n, cs, new IntType() );
	}
	
	
	/**
	 * Translate concorde result into array that associates the array index with sections in the original matrix
	 * @param tspResultFileName path to the output of the concorde output
	 * @param n number of sections
	 * @param cs charset for text file
	 * @return int[] that associates with each index (of the target matrix) the reference section from the original matrix, return value is null in case of exception
	 */
	public static int[] tspResultToArray( final String tspResultFileName, final int n, final Charset cs, final IntType dummyIndex ) {
		final int[] result = new int[ n ];
		try {
//			final List<String> lines = Files.readAllLines( Paths.get( tspResultFileName), cs);
			final ArrayList<String> lines = new ArrayList< String >();
			final File f = new File( tspResultFileName );
			final FileReader fr = new FileReader( f );
			final BufferedReader br = new BufferedReader( fr );
			String line = null;
			while ( ( line = br.readLine() ) != null )
				lines.add( line );
			// first line is number of variables, which must be n+1 because of dummy variable in TSP
			final int nVariables = Integer.parseInt( lines.get( 0 ) );
			if (nVariables != n + 1) {
				logInfo("WARNING: nVariables != n+1: " + nVariables);
				return null; // TODO Something better than returning null?
			}
			int targetIndex = 0;
			// loop through result and add numbers into result array in the order in which they appear
			// ignore dummy variable with index n
			for ( int listIndex = 1; listIndex < lines.size(); ++listIndex ) {
				final String[] currSplit = lines.get( listIndex ).split( " " );
				for ( final String s : currSplit ) {
					final int val = Integer.parseInt( s );
					// dummy variable has index n ~> ignore
					if ( val == n ) {
						dummyIndex.set( targetIndex );
						continue;
					}
					result[targetIndex] = val;
					++targetIndex;
				}
			}
		} catch (final IOException e) {
			e.printStackTrace();
			return null; // TODO Something better than returning null?
		}

		return result;
	}
	
	
	/**
	 * Translate concorde result into array that associates the array index with sections in the original matrix
	 * The indices will be shifted such that the dummy element would be at position -1.
	 * @param tspResultFileName path to the output of the concorde output
	 * @param n number of sections
	 * @return int[] that associates with each index (of the target matrix) the reference section from the original matrix, return value is null in case of exception
	 */
	public static int[] tspResultToArrayRespectDummyNode( final String tspResultFileName, final int n ) {
		return tspResultToArrayRespectDummyNode( tspResultFileName, n, Charset.defaultCharset() );
	}
	
	
	/**
	 * Translate concorde result into array that associates the array index with sections in the original matrix
	 * The indices will be shifted such that the dummy element would be at position -1.
	 * @param tspResultFileName path to the output of the concorde output
	 * @param n number of sections
	 * @param cs charset for text file
	 * @return int[] that associates with each index (of the target matrix) the reference section from the original matrix, return value is null in case of exception
	 */
	public static int[] tspResultToArrayRespectDummyNode( final String tspResultFileName, final int n, final Charset cs ) {
		
		final IntType dummyIndexObject = new IntType();
		final int[] result            = tspResultToArray( tspResultFileName, n, cs, dummyIndexObject ); // get ordering and position in array of dummy node
		final int dummyIndex          = dummyIndexObject.get();
		
		// if proper result has been returned, shift
		if ( result != null && dummyIndex < n ) {
			final int[] tmp = result.clone();
			for ( int i = 0; i < tmp.length; ++i ) {
				final int index = ( i - dummyIndex + n ) % n;
				result[ index ] = tmp[ i ];
			}
		}
		
		// reorder such that result[0] < result[result.length - 1], i.e. just invert array
		if ( result[ 0 ] > result[ result.length - 1 ] ) {
			final int halfLength = result.length / 2;
			for ( int i = 0; i < halfLength; ++i ) {
				final int fromEnd = result.length - i - 1;
				final int tmp     = result[ i ];
				result[ i ]      = result[ fromEnd ];
				result[fromEnd ] = tmp;
			}
		}
		
		return result; 
	}
	
	
	/**
	 * Rearrange matrix according to order predicted by TSP solution
	 * @param input original matrix
	 * @param associations array of index associations from TSP solution
	 */
	public static < T extends RealType< T > & NativeType< T > >  RandomAccessibleInterval< T > rearrangeMatrix(
			final RandomAccessibleInterval< T > input,
			final int[] associations) {
		return rearrangeMatrix(input, associations, new ArrayImgFactory<T>());
	}
	
	
	/**
	 * Rearrange matrix according to order predicted by TSP solution
	 * @param input original matrix
	 * @param associations array of index associations from TSP solution
	 * @param factory {@link ImgFactory} for creating output image
	 */
	public static < T extends RealType< T > & NativeType< T > >  RandomAccessibleInterval< T > rearrangeMatrix(
			final RandomAccessibleInterval< T > input,
			final int[] associations,
			final ImgFactory< T > factory ) {
		final Img<T> output = factory.create( input, input.randomAccess().get() );
		rearrangeMatrix(input, output, associations);
		return output;
	}
	
	
	/**
	 * Rearrange matrix according to order predicted by TSP solution
	 * @param input original matrix
	 * @param output output matrix, same dimensions as input; content will be overwritten
	 * @param associations array of index associations from TSP solution
	 */
	public static < T extends RealType< T > & NativeType< T > >  void rearrangeMatrix(
			final RandomAccessibleInterval< T > input,
			final RandomAccessibleInterval< T > output,
			final int[] associations )
	{
		final Cursor<T> c       = Views.flatIterable( output ).cursor();
		final RandomAccess<T> r = input.randomAccess();
		
		while( c.hasNext() ) {
			c.fwd();
			// index of array is also index of target matrix;
			// value of array at index is index of source matrix
			final int xTrans = associations[ c.getIntPosition( 0 ) ];
			final int yTrans = associations[ c.getIntPosition( 1 ) ];
			r.setPosition( xTrans, 0 );
			r.setPosition( yTrans, 1 );
			c.get().set( r.get() );
		}
	}

	private static void logInfo(final String message) {
		System.out.println("\n" + (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())) + " " + message);
	}

}







