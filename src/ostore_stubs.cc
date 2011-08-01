
#include <leveldb/db.h>
#include <leveldb/comparator.h>

extern "C" {

#include <caml/mlvalues.h>
#include <caml/memory.h>

const char cMetadata_prefix = 0;
const char cDatum_prefix = 1;

static int64_t
decode_var_int(const char *p)
{
 int64_t n = 0;
 int shift = 0;

 do {
     n += (*p & 0x7F) << shift;
     shift += 7;
 } while (*p++ & 0x80);

 return n;
}

class OStoreComparator1 : public leveldb::Comparator {
    public:
    OStoreComparator1() {}

    virtual const char* Name() const {
        return "org.eigenclass/OStoreComparator1";
    }

    virtual int Compare(const leveldb::Slice& a, const leveldb::Slice& b) const {
        size_t sa = a.size(), sb = b.size();

        if(!sa && sb) return -1;
        if(!sb && sa) return 1;
        if(!sa && !sb) return 0;

        if(a[0] != b[0]) return (a[0] - b[0]);

        switch(a[0]) {
            case '1': {
                // first compare keyspaces
                int kscmp = a[1] - b[1];
                if(kscmp) return kscmp;

                // then compare table names
                int tlen_a = a[sa - 2],
                    tlen_b = b[sb - 2];

                leveldb::Slice ta(a.data() + 2, tlen_a),
                               tb(b.data() + 2, tlen_b);
                int tblcmp = ta.compare(tb);
                if(tblcmp) return tblcmp;

                // then decode column and key lengths, then compare the
                // latter, then the former

                int last_a = a[sa - 1], last_b = b[sb - 1];
                int clen_len_a = last_a & 0x7,
                    clen_len_b = last_b & 0x7;
                int klen_len_a = (last_a >> 3) & 0x7,
                    klen_len_b = (last_b >> 3) & 0x7;

                int64_t klen_a, klen_b;

                klen_a = decode_var_int(a.data() + sa - 2 - clen_len_a - klen_len_a);
                klen_b = decode_var_int(b.data() + sb - 2 - clen_len_b - klen_len_b);

                leveldb::Slice key_a(a.data() + 2 + tlen_a, klen_a),
                               key_b(b.data() + 2 + tlen_b, klen_b);

                int keycmp = key_a.compare(key_b);
                if(keycmp) return keycmp;

                int64_t clen_a, clen_b;
                clen_a = decode_var_int(a.data() + sa - 2 - clen_len_a);
                clen_b = decode_var_int(b.data() + sb - 2 - clen_len_b);

                leveldb::Slice col_a(a.data() + 2 + tlen_a + klen_a, clen_a),
                               col_b(b.data() + 2 + tlen_b + klen_b, clen_b);

                return(col_a.compare(col_b));

                break;
            }
            default:
                // bytewise comparison for all others
                return a.compare(b);
        }

    }

    virtual void FindShortestSeparator(std::string *start,
                                       const leveldb::Slice& limit) const {
        return;
    }

    virtual void FindShortSuccessor(std::string *key) const {
        return;
    }
};

static const OStoreComparator1 comparator1;

CAMLprim const leveldb::Comparator*
ostore_custom_comparator() {
    return &comparator1;
}

CAMLprim value
ostore_apply_custom_comparator(value s1, value s2)
{
 CAMLparam2(s1, s2);
 leveldb::Slice d1(String_val(s1), string_length(s1)),
                d2(String_val(s2), string_length(s2));

 CAMLreturn(Val_int(comparator1.Compare(d1, d2)));
}

}
