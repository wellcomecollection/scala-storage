package uk.ac.wellcome.storage.listing.s3

import uk.ac.wellcome.storage.listing.Listing
import uk.ac.wellcome.storage.s3.S3ObjectLocationPrefix

trait S3Listing[ListingResult]
    extends Listing[S3ObjectLocationPrefix, ListingResult]
