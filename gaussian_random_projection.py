import numpy as np
import pandas as pd
import os
import gc
import csv
from sklearn.random_projection import GaussianRandomProjection
from sklearn.random_projection import johnson_lindenstrauss_min_dim


def calculate_target_dimension(n_samples, eps=0.5):
    """
    Calculate the minimum target dimension based on Johnson-Lindenstrauss lemma
    to preserve pairwise distances with error tolerance eps.

    Args:
        n_samples: Number of samples in the dataset
        eps: Relative error tolerance (default 0.5)

    Returns:
        int: Minimum target dimension
    """
    min_dim = johnson_lindenstrauss_min_dim(n_samples, eps=eps)
    return min_dim


def fit_projection_on_batch(batch_path, n_components=None, eps=0.5, random_state=None):
    """
    Fit Gaussian Random Projection on a batch file to create the projection transformer.

    This function reads a batch file, fits the projection, and returns the fitted transformer.
    The transformer can then be applied to all batch files using apply_projection_to_batch().

    Args:
        batch_path: Path to batch CSV file to use for fitting
        n_components: Target dimensionality. If None, auto-calculates using JL lemma.
        eps: Error tolerance for auto-calculating dimensions (default 0.5)
        random_state: Random seed for reproducibility

    Returns:
        tuple: (projection_transformer, original_shape, n_components_used)
            - projection_transformer: Fitted GaussianRandomProjection transformer
            - original_shape: (n_samples, n_features) of the fitting batch
            - n_components_used: The actual n_components used
    """
    try:
        print(f"Fitting projection on batch: {batch_path}")

        # Check if file exists
        if not os.path.exists(batch_path):
            print(f"Error: Batch file not found at {batch_path}")
            return None

        # Read batch
        df_batch = pd.read_csv(batch_path)
        invoice_no_col = df_batch.iloc[:, 0]
        X_batch = df_batch.iloc[:, 1:].values  # Features (binary matrix)

        original_shape = X_batch.shape
        print(
            f"Batch shape: {original_shape[0]} samples × {original_shape[1]} features"
        )

        # Auto-calculate target dimension if not provided
        if n_components is None:
            print(
                f"\nCalculating target dimension using Johnson-Lindenstrauss lemma..."
            )
            print(f"  Error tolerance (eps): {eps}")
            min_dim = calculate_target_dimension(original_shape[0], eps=eps)
            n_components = min_dim
            print(f"  Recommended minimum dimension: {min_dim}")
            print(f"  Using n_components = {n_components}")

        # Validate target dimension
        if n_components >= original_shape[1]:
            print(
                f"\nWarning: Target dimension ({n_components}) >= original dimension ({original_shape[1]})"
            )
            print(
                "Setting n_components to original dimension - 1 for meaningful reduction"
            )
            n_components = max(1, original_shape[1] - 1)

        if n_components <= 0:
            print(f"Error: Invalid n_components: {n_components}")
            return None

        print(f"\nFitting Gaussian Random Projection...")
        print(f"  Original dimensions: {original_shape[0]} × {original_shape[1]}")
        print(f"  Target dimensions: {original_shape[0]} × {n_components}")
        print(f"  Compression ratio: {original_shape[1]/n_components:.2f}x")

        # Create and fit Gaussian Random Projection
        grp = GaussianRandomProjection(
            n_components=n_components, random_state=random_state
        )
        grp.fit(X_batch)

        print(f"  Projection fitted successfully!")

        # Clean up memory
        del X_batch, invoice_no_col
        gc.collect()

        return grp, original_shape, n_components

    except Exception as e:
        print(f"Error fitting projection on batch: {str(e)}")
        import traceback

        traceback.print_exc()
        return None


def apply_projection_to_batch(batch_path, projection_transformer, output_path=None):
    """
    Apply a pre-fitted projection to a batch file.

    Useful for streaming simulation - apply the same projection to monthly batches.

    Args:
        batch_path: Path to batch CSV file
        projection_transformer: Fitted GaussianRandomProjection transformer
        output_path: Path to save projected batch. If None, creates _reduced suffix.

    Returns:
        pd.DataFrame: Reduced batch matrix
    """
    try:
        print(f"Applying projection to batch: {batch_path}")

        # Read batch
        df_batch = pd.read_csv(batch_path)
        invoice_no_col = df_batch.iloc[:, 0]
        X_batch = df_batch.iloc[:, 1:].values

        # Apply projection
        X_batch_reduced = projection_transformer.transform(X_batch)

        # Create reduced DataFrame
        n_components = X_batch_reduced.shape[1]
        reduced_columns = [f"RP_{i}" for i in range(n_components)]
        df_batch_reduced = pd.DataFrame(X_batch_reduced, columns=reduced_columns)
        df_batch_reduced.insert(0, "InvoiceNo", invoice_no_col.values)

        # Save if output_path provided
        if output_path:
            df_batch_reduced.to_csv(output_path, index=False)
            print(f"Reduced batch saved to: {output_path}")

        return df_batch_reduced

    except Exception as e:
        print(f"Error applying projection to batch: {str(e)}")
        import traceback

        traceback.print_exc()
        return None


def process_all_batches(
    batches_dir=None,
    first_batch_path=None,
    n_components=None,
    eps=0.5,
    random_state=None,
    output_dir=None,
):
    """
    Process all batch files using Gaussian Random Projection.

    Steps:
    1. Fits the projection on the first batch (or specified batch)
    2. Applies the same projection to all batch files
    3. Saves reduced batches to output directory

    Args:
        batches_dir: Directory containing batch files. If None, auto-detects.
        first_batch_path: Path to batch file to use for fitting. If None, uses first batch chronologically.
        n_components: Target dimensionality. If None, auto-calculates using JL lemma.
        eps: Error tolerance for auto-calculating dimensions (default 0.5)
        random_state: Random seed for reproducibility
        output_dir: Directory to save reduced batches. If None, creates 'gaussian_batches_reduced' directory.

    Returns:
        dict: Results summary with projection info and processed batch paths
    """
    try:
        # Get the current directory (Colab-friendly)
        try:
            current_dir = os.path.dirname(os.path.abspath(__file__))
        except NameError:
            current_dir = os.getcwd()

        # Auto-detect batches directory
        if batches_dir is None:
            batches_dir = os.path.join(current_dir, "batches")

        if not os.path.exists(batches_dir):
            print(f"Error: Batches directory not found at {batches_dir}")
            return None

        # Get all batch files
        batch_files = [
            os.path.join(batches_dir, f)
            for f in os.listdir(batches_dir)
            if f.startswith("batch_") and f.endswith(".csv")
        ]
        batch_files.sort()  # Sort chronologically

        if not batch_files:
            print(f"Error: No batch files found in {batches_dir}")
            return None

        print(f"Found {len(batch_files)} batch files")

        # Determine which batch to use for fitting
        if first_batch_path is None:
            first_batch_path = batch_files[0]

        print(
            f"\n=== STEP 1: Fitting projection on {os.path.basename(first_batch_path)} ==="
        )
        fit_result = fit_projection_on_batch(
            first_batch_path,
            n_components=n_components,
            eps=eps,
            random_state=random_state,
        )

        if fit_result is None:
            return None

        projection_transformer, original_shape, n_components_used = fit_result

        # Create output directory
        if output_dir is None:
            output_dir = os.path.join(current_dir, "gaussian_batches_reduced")
        os.makedirs(output_dir, exist_ok=True)

        print(f"\n=== STEP 2: Applying projection to all batches ===")
        print(f"Output directory: {output_dir}\n")

        processed_batches = []
        total_rows_processed = 0

        for batch_path in batch_files:
            batch_filename = os.path.basename(batch_path)
            output_filename = batch_filename.replace(".csv", "_reduced.csv")
            output_path = os.path.join(output_dir, output_filename)

            # Apply projection to batch
            df_reduced = apply_projection_to_batch(
                batch_path, projection_transformer, output_path=output_path
            )

            if df_reduced is not None:
                processed_batches.append(output_path)
                total_rows_processed += len(df_reduced)
                print(f"  ✓ {batch_filename} -> {output_filename}")

        print(f"\n=== PROCESSING SUMMARY ===")
        print(f"Batches processed: {len(processed_batches)}/{len(batch_files)}")
        print(f"Total rows processed: {total_rows_processed:,}")
        print(f"Original dimensions: {original_shape[1]} features")
        print(f"Reduced dimensions: {n_components_used} features")
        print(f"Compression ratio: {original_shape[1] / n_components_used:.2f}x")
        print(
            f"Memory reduction: {((original_shape[1] - n_components_used) / original_shape[1] * 100):.2f}%"
        )
        print(f"\nReduced batches saved to: {output_dir}")

        return {
            "projection_transformer": projection_transformer,
            "original_shape": original_shape,
            "n_components": n_components_used,
            "processed_batches": processed_batches,
            "output_dir": output_dir,
        }

    except Exception as e:
        print(f"Error processing batches: {str(e)}")
        import traceback

        traceback.print_exc()
        return None


if __name__ == "__main__":
    print("=== GAUSSIAN RANDOM PROJECTION ON BATCHES ===")
    print("Dimensionality reduction using Gaussian Random Projection on batch files\n")

    # Process all batches
    result = process_all_batches(random_state=42)

    if result:
        print("\n✅ Gaussian Random Projection on batches completed successfully!")
        print(f"\nReduced batches directory: {result['output_dir']}")
        print(f"Original dimensions: {result['original_shape'][1]} features")
        print(f"Reduced dimensions: {result['n_components']} features")
        print(
            f"\nCompression: {result['original_shape'][1]} → {result['n_components']} dimensions"
        )
        print(
            f"({((result['original_shape'][1] - result['n_components']) / result['original_shape'][1] * 100):.1f}% reduction)"
        )
        print(f"\nProcessed {len(result['processed_batches'])} batch files")
    else:
        print("\n❌ Gaussian Random Projection on batches failed!")
