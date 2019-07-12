package uk.ac.wellcome.storage.listing.s3

import uk.ac.wellcome.storage.ObjectLocationPrefix
import uk.ac.wellcome.storage.listing.Listing

trait S3Listing[ListingResult]
    extends Listing[ObjectLocationPrefix, ListingResult]
