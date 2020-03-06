class Mutation:

    def __init__(self, chrom: int = None, start: int = None, alt: str = None, _id: str = None):
        """
        :param int chrom:
        :param int start:
        :param str alt:
        :param str _id:
        :return:
        """
        if (chrom is None or start is None or alt is None) and _id is None:
            raise ValueError('Cannot identify Mutation. One between ID and (chrom, start, alt) must be '
                             'provided')
        self.id = _id
        self.chrom = chrom
        self.start = start
        self.alt = alt


def from_dict(mutation_dict: dict):
    if mutation_dict.get('id') is not None:
        return Mutation(_id=mutation_dict['id'])
    elif mutation_dict.get('chrom') is not None and mutation_dict.get('start') is not None and mutation_dict.get('alt') is not None:
        return Mutation(chrom=mutation_dict['chrom'], start=mutation_dict['start'], alt=mutation_dict['alt'])
    else:
        raise ValueError('Cannot identify Mutation. One between ID and (chrom, start, alt) must be '
                         'provided')
