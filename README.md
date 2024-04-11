## tikv-util

Utility to batch delete KV pairs in TiKv database by prefix.

### Run
This will delete all `blockmeta_v1_cl;****` keys in `chiado` cluster:
```bash
> tikv-util chiado blockmeta_v1_cl blockmeta_v2_cl
```

