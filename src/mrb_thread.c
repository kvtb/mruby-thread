#include <mruby.h>
#include <mruby/string.h>
#include <mruby/array.h>
#include <mruby/hash.h>
#include <mruby/range.h>
#include <mruby/proc.h>
#include <mruby/data.h>
#include <mruby/class.h>
#include <mruby/value.h>
#include <mruby/variable.h>
#include <mruby/dump.h>
#include <mruby/error.h>
#include <../mrbgems/mruby-io/include/mruby/ext/io.h>
#include <string.h>
#include <sys/time.h>
#ifndef _MSC_VER
#include <strings.h>
#include <unistd.h>
#endif
#ifdef _WIN32
#include <windows.h>
#define _TIMESPEC_DEFINED
#endif
#include <ctype.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>

#if MRUBY_RELEASE_NO > 30100
# include <mruby/internal.h>
#endif

/*
For backward compatibility.
See also https://github.com/mruby/mruby/commit/79a621dd739faf4cc0958e11d6a887331cf79e48
*/
#ifdef mrb_range_ptr
#define MRB_RANGE_PTR(v) mrb_range_ptr(v)
#else
#define MRB_RANGE_PTR(v) mrb_range_ptr(mfrom, v)
#endif

#ifdef MRB_PROC_ENV
# define _MRB_PROC_ENV(p) (p)->e.env
#else
# define _MRB_PROC_ENV(p) (p)->env
#endif

#ifndef MRB_PROC_SET_TARGET_CLASS
# define MRB_PROC_SET_TARGET_CLASS(p,tc) \
  p->target_class = tc
#endif

typedef struct {
  int           argc;
  mrb_value*    argv;
  struct RProc* proc;
  pthread_t     thread;
  mrb_state*    mrb_caller;
  mrb_state*    mrb;
  mrb_value     result;
  mrb_bool      error;  // thread ended with unhandled exception which is in `result` then
  mrb_bool      alive;
} mrb_thread_context;

static void
check_pthread_error(mrb_state *mrb, int res) {
  if (res == 0) { return; }
  mrb_raise(mrb, mrb_class_get(mrb, "ThreadError"), strerror(res));
}

static void
mrb_thread_context_free(mrb_state *mrb, void *p) {
  if (p) {
    mrb_thread_context* context = (mrb_thread_context*) p;
    if (context->alive) {
      pthread_cancel(context->thread);
      pthread_join(context->thread, NULL); // do not do `free` until thread is ended
    }
    if (context->mrb && context->mrb != mrb) mrb_close(context->mrb);
    if (context->argv) free(context->argv);
    free(p);
  }
}

static const struct mrb_data_type mrb_thread_context_type = {
  "mrb_thread_context", mrb_thread_context_free,
};

typedef struct {
  pthread_mutex_t mutex;
  int locked;
} mrb_mutex_context;

static void
mrb_mutex_context_free(mrb_state *mrb, void *p) {
  if (p) {
    mrb_mutex_context* context = (mrb_mutex_context*) p;
    pthread_mutex_destroy(&context->mutex);
    free(p);
  }
}

static const struct mrb_data_type mrb_mutex_context_type = {
  "mrb_mutex_context", mrb_mutex_context_free,
};

typedef struct {
  pthread_mutex_t mutex, queue_lock;
  pthread_cond_t cond;
  mrb_state* mrb;
  mrb_value queue;
} mrb_queue_context;

static void
mrb_queue_context_free(mrb_state *mrb, void *p) {
  if (p) {
    mrb_queue_context* context = (mrb_queue_context*) p;
    pthread_cond_destroy(&context->cond);
    pthread_mutex_destroy(&context->mutex);
    pthread_mutex_destroy(&context->queue_lock);
    if (context->mrb)
      mrb_close(context->mrb);
    free(p);
  }
}

static const struct mrb_data_type mrb_queue_context_type = {
  "mrb_queue_context", mrb_queue_context_free,
};

mrb_value mrb_thread_migrate_value(mrb_state *mexc, mrb_state *mfrom, mrb_value v, mrb_state *mto);

static mrb_sym
migrate_sym(mrb_state *mfrom, mrb_sym sym, mrb_state *mto)
{
  mrb_int len;
  const char *p = mrb_sym2name_len(mfrom, sym, &len);
  return mrb_intern_static(mto, p, len);
}

static void
migrate_all_symbols(mrb_state *mfrom, mrb_state *mto)
{
  mrb_sym i;
  for (i = 1; i < mfrom->symidx + 1; i++) {
    migrate_sym(mfrom, i, mto);
  }
}

static void
migrate_simple_iv(mrb_state *mexc, mrb_state *mfrom, mrb_value v, mrb_state *mto, mrb_value v2)
{
  mrb_value ivars = mrb_obj_instance_variables(mfrom, v);
  mrb_value iv;
  mrb_int i;

//mrb_warn(mfrom, "migrate_simple_iv RARRAY_LEN(ivars)=%d", RARRAY_LEN(ivars));
  for (i=0; i<RARRAY_LEN(ivars); i++) {
    mrb_sym sym = mrb_symbol(RARRAY_PTR(ivars)[i]);
    mrb_sym sym2 = migrate_sym(mfrom, sym, mto);
//  mrb_warn(mfrom, "migrate_simple_iv %v::%n", v, sym);
    iv = mrb_iv_get(mfrom, v, sym);
    mrb_iv_set(mto, v2, sym2, mrb_thread_migrate_value(mexc, mfrom, iv, mto));
  }
}

static mrb_bool
is_safe_migratable_datatype(const mrb_data_type *type)
{
  static const char *known_type_names[] = {
    "mrb_thread_context",
    "mrb_mutex_context",
    "mrb_queue_context",
    "IO",
    "Time",
    NULL
  };
  int i;
  for (i = 0; known_type_names[i]; i++) {
    if (strcmp(type->struct_name, known_type_names[i]) == 0)
      return TRUE;
  }
  return FALSE;
}

static mrb_bool
is_safe_migratable_simple_value(mrb_state *mfrom, mrb_value v, mrb_state *mto)
{
  switch (mrb_type(v)) {
  case MRB_TT_OBJECT:
  case MRB_TT_EXCEPTION:
    {
      struct RObject *o = mrb_obj_ptr(v);
      mrb_value path = mrb_class_path(mfrom, o->c);

      if (mrb_nil_p(path) || !mrb_class_defined(mto, RSTRING_PTR(path))) {
        return FALSE;
      }
    }
    break;
  case MRB_TT_PROC:
  case MRB_TT_FALSE:
  case MRB_TT_TRUE:
  case MRB_TT_FIXNUM:
  case MRB_TT_SYMBOL:
#ifndef MRB_WITHOUT_FLOAT
  case MRB_TT_FLOAT:
#endif
  case MRB_TT_STRING:
    break;
  case MRB_TT_RANGE:
    {
      struct RRange *r = MRB_RANGE_PTR(v);
      if (!is_safe_migratable_simple_value(mfrom, RANGE_BEG(r), mto) ||
          !is_safe_migratable_simple_value(mfrom, RANGE_END(r), mto)) {
        return FALSE;
      }
    }
    break;
  case MRB_TT_ARRAY:
    {
      int i;
      for (i=0; i<RARRAY_LEN(v); i++) {
        if (!is_safe_migratable_simple_value(mfrom, RARRAY_PTR(v)[i], mto)) {
          return FALSE;
        }
      }
    }
    break;
  case MRB_TT_HASH:
    {
      mrb_value ka;
      int i, l;
      ka = mrb_hash_keys(mfrom, v);
      l = RARRAY_LEN(ka);
      for (i = 0; i < l; i++) {
        mrb_value k = mrb_ary_entry(ka, i);
        if (!is_safe_migratable_simple_value(mfrom, k, mto) ||
            !is_safe_migratable_simple_value(mfrom, mrb_hash_get(mfrom, v, k), mto)) {
          return FALSE;
        }
      }
    }
    break;
  case MRB_TT_DATA:
    return is_safe_migratable_datatype(DATA_TYPE(v));
  default:
    return FALSE;
  }
  return TRUE;
}

static void
migrate_irep_child(mrb_state *mfrom, /*const*/ mrb_irep *ret, mrb_state *mto)
{
  int i;
  mrb_code *old_iseq;

  // migrate pool
  // FIXME: broken with mruby3
  #ifndef IREP_TT_SFLAG
  for (i = 0; i < ret->plen; ++i) {
    mrb_value v = ret->pool[i];
    if (mrb_type(v) == MRB_TT_STRING) {
      struct RString *s = mrb_str_ptr(v);
      if (RSTR_NOFREE_P(s) && RSTRING_LEN(v) > 0) {
        char *old = RSTRING_PTR(v);
        s->as.heap.ptr = (char*)mrb_malloc(mto, RSTRING_LEN(v));
        memcpy(s->as.heap.ptr, old, RSTRING_LEN(v));
        RSTR_UNSET_NOFREE_FLAG(s);
      }
    }
  }
  #endif

  // migrate iseq
  if (ret->flags & MRB_ISEQ_NO_FREE) {
    old_iseq = ret->iseq;
    ret->iseq = (mrb_code*)mrb_malloc(mto, sizeof(mrb_code) * ret->ilen);
    memcpy(ret->iseq, old_iseq, sizeof(mrb_code) * ret->ilen);
    ret->flags &= ~MRB_ISEQ_NO_FREE;
  }

  // migrate sub ireps
  for (i = 0; i < ret->rlen; ++i) {
    migrate_irep_child(mfrom, ret->reps[i], mto);
  }
}

static mrb_irep*
migrate_irep(mrb_state *mfrom, const mrb_irep *src, mrb_state *mto) {
  uint8_t *irep = NULL;
  size_t binsize = 0;
  mrb_irep *ret;
#ifdef DUMP_ENDIAN_NAT
  mrb_dump_irep(mfrom, src, DUMP_ENDIAN_NAT, &irep, &binsize);
#else
  mrb_dump_irep(mfrom, src, 0, &irep, &binsize);
#endif

  ret = mrb_read_irep(mto, irep);
  migrate_irep_child(mfrom, ret, mto);
  mrb_free(mfrom, irep);
  return ret;
}

struct RProc*
migrate_rproc(mrb_state *mexc, mrb_state *mfrom, const struct RProc *rproc, mrb_state *mto) {
  struct RProc *newproc;

  if (MRB_PROC_ALIAS_P(rproc)) { // `rproc->body.mid`
    mrb_raisef(mexc, mrb_class_get(mexc, "ArgumentError"), "TODO: migrate alias proc");
  } else if (MRB_PROC_CFUNC_P(rproc)) {
    newproc = mrb_proc_new_cfunc(mto, rproc->body.func);
  } else {
    newproc = mrb_proc_new(mto, migrate_irep(mfrom, rproc->body.irep, mto));
    mrb_irep_decref(mto, newproc->body.irep);
  }

#ifdef MRB_PROC_ENV_P
  if (_MRB_PROC_ENV(rproc) && MRB_PROC_ENV_P(rproc)) {
#else
  if (_MRB_PROC_ENV(rproc)) {
#endif
#ifdef MRB_ENV_LEN
    mrb_int i, len = MRB_ENV_LEN(_MRB_PROC_ENV(rproc));
#else
    mrb_int i, len = MRB_ENV_STACK_LEN(_MRB_PROC_ENV(rproc));
#endif
    struct REnv *newenv = (struct REnv*)mrb_obj_alloc(mto, MRB_TT_ENV, mto->object_class);

    newenv->stack = mrb_malloc(mto, sizeof(mrb_value) * len);
#ifdef MRB_ENV_CLOSE
    MRB_ENV_CLOSE(newenv);
#else
    MRB_ENV_UNSHARE_STACK(newenv);
#endif
    for (i = 0; i < len; ++i) {
      mrb_value v = _MRB_PROC_ENV(rproc)->stack[i];
      if (mrb_obj_ptr(v) == ((struct RObject*)rproc)) {
        newenv->stack[i] = mrb_obj_value(newproc);
      } else {
        newenv->stack[i] = mrb_thread_migrate_value(mexc, mfrom, v, mto);
      }
    }
#ifdef MRB_SET_ENV_STACK_LEN
    MRB_SET_ENV_STACK_LEN(newenv, len);
#elif defined MRB_ENV_SET_LEN
    MRB_ENV_SET_LEN(newenv, len);
#endif
    _MRB_PROC_ENV(newproc) = newenv;
#ifdef MRB_PROC_ENVSET
    newproc->flags |= MRB_PROC_ENVSET;
#endif
    if (rproc->upper) {
      newproc->upper = migrate_rproc(mexc, mfrom, rproc->upper, mto);
    }
  }

  return newproc;
}

static struct RClass*
path2class(mrb_state *mexc, mrb_state *mfrom, mrb_value vfrom, mrb_state *mto, char const* path_begin, mrb_int len) {
  char const* begin = path_begin;
  char const* p = begin;
  char const* end = begin + len;
  struct RClass* ret = mto->object_class;

  while(1) {
    mrb_sym cls;
    mrb_value cnst;

    while((p < end && p[0] != ':') ||
          ((p + 1) < end && p[1] != ':')) ++p;

    cls = mrb_intern(mto, begin, p - begin);
    if (!mrb_mod_cv_defined(mto, ret, cls)) {
      if (mfrom == NULL) {
        mrb_raisef(mexc, mrb_class_get(mexc, "ArgumentError"), "undefined class/module %S",
                   mrb_str_new(mexc, path_begin, p - path_begin));
      }
      if(p >= end) { // the last segment
        switch (mrb_type(vfrom)) {
        case MRB_TT_CLASS: {
          struct RClass * superclass = mrb_class_ptr(vfrom)->super;
          while (superclass && superclass->tt == MRB_TT_ICLASS) {
            superclass = superclass->super;
          }
          mrb_value superclass_path = mrb_class_path(mfrom, superclass);
          struct RClass *superclass2 = path2class(mexc,
                                                  /*mfrom=*/NULL, mrb_nil_value(),
                                                  mto,
                                                  RSTRING_PTR(superclass_path), RSTRING_LEN(superclass_path));
          return mrb_define_class_under_id(mto, ret, cls, superclass2);
        }
        case MRB_TT_MODULE:
          return mrb_define_module_under_id(mto, ret, cls);
        default:
          break;
        }
      }
      mrb_raisef(mexc, mrb_class_get(mexc, "ArgumentError"), "TODO: migrate class/module %S",
                 mrb_str_new(mexc, path_begin, p - path_begin));
    }

    cnst = mrb_mod_cv_get(mto, ret, cls);
    if (mrb_type(cnst) != MRB_TT_CLASS && mrb_type(cnst) != MRB_TT_MODULE) {
      mrb_raisef(mexc, mrb_class_get(mexc, "TypeError"), "%S does not refer to class/module",
                 mrb_str_new(mexc, path_begin, p - path_begin));
    }
    ret = mrb_class_ptr(cnst);

    if(p >= end) { break; }

    p += 2;
    begin = p;
  }
  return ret;
}

// defined in  mrbgems/mruby-time/include/mruby/time.h
enum mrb_timezone { TZ_NONE = 0 };
// copied from mrbgems/mruby-time/src/time.c
struct mrb_time {
  time_t              sec;
  time_t              usec;
  enum mrb_timezone   timezone;
  struct tm           datetime;
};


struct hash_callback_data {
  mrb_state *mexc;
  mrb_state *mto;
  mrb_value nv;
};

static int
hash_callback(mrb_state *mfrom, mrb_value key, mrb_value val, void *data) {
  struct hash_callback_data *hcbd = (struct hash_callback_data *)data;

  mrb_value k2 = mrb_thread_migrate_value(hcbd->mexc, mfrom, key, hcbd->mto);
  mrb_value v2 = mrb_thread_migrate_value(hcbd->mexc, mfrom, val, hcbd->mto);
  mrb_hash_set(hcbd->mto, hcbd->nv, k2, v2);

  return 0;
}

struct migrate_method_data {
  mrb_state     *mexc;
  mrb_state     *mto;
  struct RClass *c2;       // = mrb_class_ptr(v2);
  mrb_value      v2;       // = mrb_obj_value(c2);
};

int migrate_methods_foreach_func(mrb_state* mfrom, mrb_sym sym, mrb_method_t m, void *data) {
  struct migrate_method_data *mmd = (struct migrate_method_data *)data;

//mrb_warn(mfrom, "mt.sym = %n", sym);
  mrb_sym sym2 = migrate_sym(mfrom, sym, mmd->mto);

  struct RClass * cc2 = mmd->c2;
  mrb_method_t m2 = mrb_method_search_vm(mmd->mto, &cc2, sym2);
  if (MRB_METHOD_UNDEF_P(m2) || cc2 != mmd->c2) { /* not found or found in supers */
    if (MRB_METHOD_PROC_P(m)) {
      const struct RProc *p1 = MRB_METHOD_PROC(m);
      if (p1) {
//      mrb_warn(mmd->mto, "migrate method %v::%n", mrb_class_path(mmd->mto, mmd->c2), sym2);
        int ai = mrb_gc_arena_save(mmd->mto);
        const struct RProc *p2 = migrate_rproc(mmd->mexc, mfrom, p1, mmd->mto);

        mrb_method_t m3;
        MRB_METHOD_FROM_PROC(m3, p2);
        mrb_define_method_raw(mmd->mto, mmd->c2, sym2, m3);
        mrb_gc_arena_restore(mmd->mto, ai);
      } else {
//      mrb_warn(mmd->mto, "no migrate method %v::%n because proc==NULL", mrb_class_path(mmd->mto, mmd->c2), sym2);
      }
    } else {
//    mrb_warn(mmd->mto, "no migrate method %v::%n because not proc", mrb_class_path(mmd->mto, mmd->c2), sym2);
    }
  } else {
  //mrb_warn(mmd->mto, "no migrate method %v::%n because target has it", mrb_class_path(mmd->mto, mmd->c2), sym2); // but target always have `to_s`?
  }
  return 0;
}

int migrate_vars_foreach_func(mrb_state* mfrom, mrb_sym sym, mrb_value iv, void *data) {
  struct migrate_method_data *mmd = (struct migrate_method_data *)data;
  mrb_sym sym2 = migrate_sym(mfrom, sym, mmd->mto);

//mrb_warn(mmd->mto, "iv.sym = %v::%n", mrb_class_path(mmd->mto, mmd->c2), sym2);

  if (mrb_iv_defined(mmd->mto, mmd->v2, sym2)) {
    if (mrb_type(iv) == MRB_TT_CLASS || mrb_type(iv) == MRB_TT_MODULE) {
      // these may have nested methods/constants missing in `mto`, so go deeper recursively
      mrb_value iv2 = mrb_iv_get(mmd->mto, mmd->v2, sym2);
//    mrb_warn(mmd->mto, "iv2 = %v :: %Y", iv2, iv2);
      if (mrb_type(iv) != mrb_type(iv2)) {
        mrb_raisef(mmd->mexc, mrb_class_get(mmd->mexc, "TypeError"), "different types");
      }
      struct RClass * c3 = mrb_class_ptr(iv2);
      if (mmd->c2 == c3) {
         // prevent infinite recursion on Object::Object
      } else {
//      mrb_warn(mmd->mto, "c3 = %v", mrb_class_path(mmd->mto, c3));

        struct migrate_method_data mmd2 = { mmd->mexc, mmd->mto, c3, iv2 };
        mrb_mt_foreach(mfrom, mrb_class_ptr(iv), migrate_methods_foreach_func, &mmd2);
        mrb_iv_foreach(mfrom, iv, migrate_vars_foreach_func, &mmd2);
      }
    } else {
  //mrb_warn(mfrom, "no migrate var %v::%n because target has it", mmd->cls_path, sym);
    }
  } else {
//  mrb_warn(mmd->mto, "migrate var %v::%n", mrb_class_path(mmd->mto, mmd->c2), sym2);
    mrb_iv_set(mmd->mto, mmd->v2, sym2, mrb_thread_migrate_value(mmd->mexc, mfrom, iv, mmd->mto));
  }
  return 0;
}

// based on https://gist.github.com/3066997
mrb_value
mrb_thread_migrate_value(mrb_state *mexc, mrb_state *mfrom, mrb_value const v, mrb_state *mto) {
  if (mfrom == mto) { return v; } // assert?

//mrb_warn(mfrom, "mrb_type(v)=%d", mrb_type(v));
  switch (mrb_type(v)) {
  case MRB_TT_CLASS:
  case MRB_TT_MODULE: {
    mrb_value cls_path = mrb_class_path(mfrom, mrb_class_ptr(v));
    if (mrb_nil_p(cls_path)) {
//    mrb_warn(mfrom, "no migrate %v :: %Y because cls_path==nil", v, v);
      return mrb_nil_value();
    }

//  mrb_warn(mfrom, "migrate TT_CLASS: %v :: %Y; cls_path1 = %v :: %Y", v, v, cls_path, cls_path);
    struct RClass *c2 = path2class(mexc,
                                   mfrom, v/* to define same class if not found in mto */,
                                   mto, RSTRING_PTR(cls_path), RSTRING_LEN(cls_path));
    mrb_value v2 = mrb_obj_value(c2);

//  mrb_warn(mto, "v2 = %v :: %Y", v2, v2);

    struct migrate_method_data mmd = { mexc, mto, c2, v2 };
    mrb_mt_foreach(mfrom, mrb_class_ptr(v), migrate_methods_foreach_func, &mmd);
    mrb_iv_foreach(mfrom, v, migrate_vars_foreach_func, &mmd);

    return v2;
  }

  case MRB_TT_OBJECT:
  case MRB_TT_EXCEPTION: {
      mrb_value cls_path = mrb_class_path(mfrom, mrb_class(mfrom, v));
      if (mrb_nil_p(cls_path)) {
//      mrb_warn(mfrom, "no migrate %v :: %Y because cls_path==nil", v, v);
        return mrb_nil_value();
      }
//    mrb_warn(mfrom, "migrate TT_OBJECT: %v :: %Y; cls_path2 = %v :: %Y", v, v, cls_path, cls_path);
      struct RClass *c2 = path2class(mexc,
                                     /*mfrom=*/NULL, mrb_nil_value(),
                                     mto, RSTRING_PTR(cls_path), RSTRING_LEN(cls_path));
      mrb_value nv = mrb_obj_value(mrb_obj_alloc(mto, mrb_type(v), c2));
      migrate_simple_iv(mexc, mfrom, v, mto, nv);
      if (mrb_type(v) == MRB_TT_EXCEPTION) {
        mrb_iv_set(mto, nv, mrb_intern_lit(mto, "mesg"),
                   mrb_thread_migrate_value(mexc, mfrom, mrb_iv_get(mfrom, v, mrb_intern_lit(mfrom, "mesg")), mto));
      }
      return nv;
    }

  case MRB_TT_PROC:
    return mrb_obj_value(migrate_rproc(mexc, mfrom, mrb_proc_ptr(v), mto));
  case MRB_TT_FALSE:
  case MRB_TT_TRUE:
  case MRB_TT_FIXNUM:
    return v;
  case MRB_TT_SYMBOL:
    return mrb_symbol_value(migrate_sym(mfrom, mrb_symbol(v), mto));
#ifndef MRB_WITHOUT_FLOAT
  case MRB_TT_FLOAT:
    return mrb_float_value(mto, mrb_float(v));
#endif
  case MRB_TT_STRING:
    return mrb_str_new(mto, RSTRING_PTR(v), RSTRING_LEN(v));

  case MRB_TT_RANGE: {
    struct RRange *r = MRB_RANGE_PTR(v);
    return mrb_range_new(mto,
                         mrb_thread_migrate_value(mexc, mfrom, RANGE_BEG(r), mto),
                         mrb_thread_migrate_value(mexc, mfrom, RANGE_END(r), mto),
                         RANGE_EXCL(r));
  }

  case MRB_TT_ARRAY: {
    int i, ai;

    mrb_value nv = mrb_ary_new_capa(mto, RARRAY_LEN(v));
    ai = mrb_gc_arena_save(mto);
    for (i=0; i<RARRAY_LEN(v); i++) {
      mrb_ary_push(mto, nv, mrb_thread_migrate_value(mexc, mfrom, RARRAY_PTR(v)[i], mto));
    }
    mrb_gc_arena_restore(mto, ai);
    return nv;
  }

  case MRB_TT_HASH: {
    struct hash_callback_data hkbd;
    hkbd.mexc = mexc;
    hkbd.mto = mto;
    hkbd.nv = mrb_hash_new(mto);

    int ai = mrb_gc_arena_save(mto);
    mrb_hash_foreach(mfrom, mrb_hash_ptr(v), hash_callback, &hkbd);
    mrb_gc_arena_restore(mto, ai);
    return hkbd.nv;
  }

  case MRB_TT_DATA: {
    mrb_value cls_path = mrb_class_path(mfrom, mrb_class(mfrom, v));
//  mrb_warn(mfrom, "migrate TT_DATA: %v :: %Y; cls_path3 = %v :: %Y", v, v, cls_path, cls_path);
    struct RClass *c2 = path2class(mexc,
                                   /*mfrom=*/NULL, mrb_nil_value(),
                                   mto, RSTRING_PTR(cls_path), RSTRING_LEN(cls_path));
    if (!is_safe_migratable_datatype(DATA_TYPE(v))) {
      mrb_raisef(mexc,
                 mrb_exc_get_id(mexc, mrb_intern_lit(mexc, "TypeError")), //E_TYPE_ERROR,
                 "cannot migrate TT_DATA object: %S(%S)",
                 mrb_str_new_cstr(mexc, DATA_TYPE(v)->struct_name), mrb_inspect(mfrom, v));
      //mrb_warn(  mexc,
      //           "cannot migrate TT_DATA object: %S(%S)",
      //           mrb_str_new_cstr(mexc, DATA_TYPE(v)->struct_name), mrb_inspect(mfrom, v));
      //return mrb_nil_value();
    }
    mrb_value nv = mrb_obj_value(mrb_obj_alloc(mto, mrb_type(v), c2));
    if (strcmp(DATA_TYPE(v)->struct_name, "Time") == 0) {
      DATA_PTR(nv) = mrb_malloc(mto, sizeof(struct mrb_time));
      *((struct mrb_time*)DATA_PTR(nv)) = *((struct mrb_time*)DATA_PTR(v));
      DATA_TYPE(nv) = DATA_TYPE(v);
    } else if (strcmp(DATA_TYPE(v)->struct_name, "IO") == 0) {
      DATA_PTR(nv) = mrb_malloc(mto, sizeof(struct mrb_io));
      *((struct mrb_io*)DATA_PTR(nv)) = *((struct mrb_io*)DATA_PTR(v));
      ((struct mrb_io*)DATA_PTR(nv))->buf = NULL; // TODO: or alloc new buf?
      DATA_TYPE(nv) = DATA_TYPE(v);
    } else {
      DATA_PTR(nv) = DATA_PTR(v);
      // Don't copy type information to avoid freeing in sub-thread.
      // DATA_TYPE(nv) = DATA_TYPE(v);
      migrate_simple_iv(mexc, mfrom, v, mto, nv);
    }
    return nv;
  }

    // case MRB_TT_FREE: return mrb_nil_value();

  default: break;
  }

  mrb_raisef(mexc,
             mrb_exc_get_id(mexc, mrb_intern_lit(mexc, "TypeError")), //E_TYPE_ERROR,
             "cannot migrate object: %S(%v)", mrb_inspect(mfrom, v), mrb_fixnum_value(mrb_type(v)));
  return mrb_nil_value();
}

static mrb_value
mrb_thread_func_body(mrb_state *mrb, void *data) {
  mrb_thread_context* context = (mrb_thread_context*) data;

  return mrb_yield_with_class(mrb, mrb_obj_value(context->proc),
                              context->argc, context->argv,
                              mrb_nil_value(), mrb->object_class);
}

static void*
mrb_thread_func(void* data) {
  mrb_thread_context* context = (mrb_thread_context*) data;
  mrb_state* mrb = context->mrb;

  context->result = mrb_protect_error(mrb, &mrb_thread_func_body, data, &context->error);
  if (context->error) {
     mrb_warn(mrb, "thread raised %Y (%v)", context->result, context->result);
  } else {
//   mrb_warn(mrb, "thread ended %v :: %Y", context->result, context->result);
  }

  mrb_gc_protect(mrb, context->result);
  context->alive = FALSE;
  return NULL;
}

static mrb_value
mrb_thread_init(mrb_state* mrb, mrb_value self) {
  static mrb_thread_context const ctx_zero = {0};

  mrb_value proc = mrb_nil_value();
  mrb_int argc;
  mrb_value* argv;

  int i, l;
  mrb_thread_context* context = (mrb_thread_context*) malloc(sizeof(mrb_thread_context));

  *context = ctx_zero;
  mrb_data_init(self, context, &mrb_thread_context_type);

  mrb_get_args(mrb, "&*", &proc, &argv, &argc);
  if (!mrb_nil_p(proc) && MRB_PROC_CFUNC_P(mrb_proc_ptr(proc))) {
    mrb_raise(mrb, E_RUNTIME_ERROR, "forking C defined block");
  }
  if (mrb_nil_p(proc)) { return self; }

  context->mrb_caller = mrb;
  context->mrb = mrb_open();
  if (!context->mrb) {
    mrb_raise(mrb, E_RUNTIME_ERROR, "mrb_open failed");
  }
//migrate_all_symbols(mrb, context->mrb); // TODO: needed?

  context->proc = migrate_rproc(mrb, mrb, mrb_proc_ptr(proc), context->mrb);
  MRB_PROC_SET_TARGET_CLASS(context->proc, context->mrb->object_class);
  context->argc = argc;
  context->argv = calloc(sizeof (mrb_value), context->argc);
  context->result = mrb_nil_value();
  context->error = FALSE;

  // move global functions, which are Object's methods actually
//mrb_warn(mrb, "migrate Object");
  mrb_thread_migrate_value(mrb, mrb, mrb_obj_value(mrb->object_class), context->mrb);

  for (i = 0; i < context->argc; i++) {
//  mrb_warn(mrb, "migrate arg %v :: %Y", argv[i], argv[i]);
    context->argv[i] = mrb_thread_migrate_value(mrb, mrb, argv[i], context->mrb);
  }

  mrb_value gv = mrb_f_global_variables(mrb, self);
  l = RARRAY_LEN(gv);
  for (i = 0; i < l; i++) {
    mrb_int len;
    int ai = mrb_gc_arena_save(mrb);
    mrb_value k = mrb_ary_entry(gv, i);
    mrb_value o = mrb_gv_get(mrb, mrb_symbol(k));
    if (is_safe_migratable_simple_value(mrb, o, context->mrb)) {
//    mrb_warn(mrb, "migrate global %v: %v :: %Y", k, o, o);
      const char *p = mrb_sym2name_len(mrb, mrb_symbol(k), &len);
      mrb_gv_set(context->mrb,
                 mrb_intern_static(context->mrb, p, len),
                 mrb_thread_migrate_value(mrb, mrb, o, context->mrb));
    } else {
//    mrb_warn(mrb, "no migrate global %v: %v :: %Y", k, o, o);
    }
    mrb_gc_arena_restore(mrb, ai);
  }

  context->alive = TRUE;
  check_pthread_error(mrb, pthread_create(&context->thread, NULL, &mrb_thread_func, (void*) context));

  return self;
}

static mrb_value
mrb_thread_join(mrb_state* mrb, mrb_value self) {
  mrb_thread_context* context = (mrb_thread_context*)DATA_PTR(self);
  check_pthread_error(mrb, pthread_join(context->thread, NULL));

  context->result = mrb_thread_migrate_value(mrb, context->mrb, context->result, mrb);
  mrb_close(context->mrb);
  context->mrb = NULL;
  if (context->error) {
    // todo: raise
  }
  return context->result;
}

static mrb_value
mrb_thread_kill(mrb_state* mrb, mrb_value self) {
  mrb_thread_context* context = (mrb_thread_context*)DATA_PTR(self);
  if (context->mrb == NULL) {
    return mrb_nil_value();
  }
  if (context->alive) {
    check_pthread_error(mrb, pthread_kill(context->thread, SIGINT));
    context->result = mrb_thread_migrate_value(mrb, context->mrb, context->result, mrb);
    if (context->error) {
      // todo: raise
    }
    return context->result;
  }
  return mrb_nil_value();
}

static mrb_value
mrb_thread_alive(mrb_state* mrb, mrb_value self) {
  mrb_thread_context* context = (mrb_thread_context*)DATA_PTR(self);
  return mrb_bool_value(context->alive);
}

static mrb_value
mrb_thread_sleep(mrb_state* mrb, mrb_value self) {
  mrb_int t;
  mrb_get_args(mrb, "i", &t);
#ifndef _WIN32
  sleep(t);
#else
  Sleep(t * 1000);
#endif
  return mrb_nil_value();
}

#ifdef _MSC_VER
static
int usleep(useconds_t usec) {
  LARGE_INTEGER pf, s, c;
  if (!QueryPerformanceFrequency(&pf))
    return -1;
  if (!QueryPerformanceCounter(&s))
    return -1;
  do {
    if (QueryPerformanceCounter((LARGE_INTEGER*) &c))
      return -1;
  } while ((c.QuadPart - s.QuadPart) / (float)pf.QuadPart * 1000 * 1000 < t);
  return 0;
}
#endif

static mrb_value
mrb_thread_usleep(mrb_state* mrb, mrb_value self) {
  mrb_int t;
  mrb_get_args(mrb, "i", &t);
  usleep(t);
  return mrb_nil_value();
}

static mrb_value
mrb_mutex_init(mrb_state* mrb, mrb_value self) {
  mrb_mutex_context* context = (mrb_mutex_context*) malloc(sizeof(mrb_mutex_context));
  check_pthread_error(mrb, pthread_mutex_init(&context->mutex, NULL));
  context->locked = FALSE;
  DATA_PTR(self) = context;
  DATA_TYPE(self) = &mrb_mutex_context_type;
  return self;
}

static mrb_value
mrb_mutex_lock(mrb_state* mrb, mrb_value self) {
  mrb_mutex_context* context = DATA_PTR(self);
  check_pthread_error(mrb, pthread_mutex_lock(&context->mutex));
  context->locked = TRUE;
  return mrb_nil_value();
}

static mrb_value
mrb_mutex_try_lock(mrb_state* mrb, mrb_value self) {
  mrb_mutex_context* context = DATA_PTR(self);
  if (pthread_mutex_trylock(&context->mutex) == 0) {
    context->locked = TRUE;
    return mrb_true_value();
  }
  return mrb_false_value();
}

static mrb_value
mrb_mutex_locked(mrb_state* mrb, mrb_value self) {
  mrb_mutex_context* context = DATA_PTR(self);
  return context->locked ? mrb_true_value() : mrb_false_value();
}

static mrb_value
mrb_mutex_unlock(mrb_state* mrb, mrb_value self) {
  mrb_mutex_context* context = DATA_PTR(self);
  check_pthread_error(mrb, pthread_mutex_unlock(&context->mutex));
  context->locked = FALSE;
  return mrb_nil_value();
}

static mrb_value
mrb_mutex_sleep(mrb_state* mrb, mrb_value self) {
  mrb_int t;
  mrb_get_args(mrb, "i", &t);
#ifndef _WIN32
  sleep(t);
#else
  Sleep(t * 1000);
#endif
  return mrb_mutex_unlock(mrb, self);
}

static mrb_value
mrb_mutex_synchronize(mrb_state* mrb, mrb_value self) {
  mrb_value ret = mrb_nil_value();
  mrb_value proc = mrb_nil_value();
  mrb_get_args(mrb, "&", &proc);
  if (!mrb_nil_p(proc)) {
    mrb_mutex_lock(mrb, self);
    ret = mrb_yield_argv(mrb, proc, 0, NULL);
    mrb_mutex_unlock(mrb, self);
  }
  return ret;
}

static mrb_value
mrb_queue_init(mrb_state* mrb, mrb_value self) {
  mrb_queue_context* context = (mrb_queue_context*) malloc(sizeof(mrb_queue_context));
  check_pthread_error(mrb, pthread_mutex_init(&context->mutex, NULL));
  check_pthread_error(mrb, pthread_cond_init(&context->cond, NULL));
  check_pthread_error(mrb, pthread_mutex_init(&context->queue_lock, NULL));
  context->mrb = mrb_open();
  if (!context->mrb) {
    mrb_raise(mrb, E_RUNTIME_ERROR, "mrb_open failed");
  }
  context->queue = mrb_ary_new(context->mrb);
  mrb_data_init(self, context, &mrb_queue_context_type);

  check_pthread_error(mrb, pthread_mutex_lock(&context->queue_lock));

  return self;
}

static mrb_value
mrb_queue_lock(mrb_state* mrb, mrb_value self) {
  mrb_queue_context* context = DATA_PTR(self);
  check_pthread_error(mrb, pthread_mutex_lock(&context->mutex));
  return mrb_nil_value();
}


static mrb_value
mrb_queue_unlock(mrb_state* mrb, mrb_value self) {
  mrb_queue_context* context = DATA_PTR(self);
  check_pthread_error(mrb, pthread_mutex_unlock(&context->mutex));
  return mrb_nil_value();
}

static mrb_value
mrb_queue_clear(mrb_state* mrb, mrb_value self) {
  mrb_queue_context* context = DATA_PTR(self);

  mrb_queue_lock(mrb, self);
  mrb_ary_clear(context->mrb, context->queue);
  mrb_queue_unlock(mrb, self);

  return mrb_nil_value();
}

static mrb_value
mrb_queue_push(mrb_state* mrb, mrb_value self) {
  mrb_value arg;
  mrb_queue_context* context = DATA_PTR(self);
  mrb_get_args(mrb, "o", &arg);

  mrb_queue_lock(mrb, self);
  mrb_ary_push(context->mrb, context->queue, mrb_thread_migrate_value(mrb, mrb, arg, context->mrb));
  mrb_queue_unlock(mrb, self);

  check_pthread_error(mrb, pthread_cond_signal(&context->cond));
  return mrb_nil_value();
}

static mrb_value
mrb_queue_pop(mrb_state* mrb, mrb_value self) {
  mrb_value ret;
  mrb_queue_context* context = DATA_PTR(self);
  int len;

  mrb_queue_lock(mrb, self);
  len = RARRAY_LEN(context->queue);
  mrb_queue_unlock(mrb, self);

  if (len == 0) {
    check_pthread_error(mrb, pthread_cond_wait(&context->cond, &context->queue_lock));
  }

  mrb_queue_lock(mrb, self);
  ret = mrb_thread_migrate_value(mrb, context->mrb, mrb_ary_pop(context->mrb, context->queue), mrb);
  mrb_queue_unlock(mrb, self);

  return ret;
}

static mrb_value
mrb_queue_unshift(mrb_state* mrb, mrb_value self) {
  mrb_value arg;
  mrb_queue_context* context = DATA_PTR(self);
  mrb_get_args(mrb, "o", &arg);

  mrb_queue_lock(mrb, self);
  mrb_ary_unshift(context->mrb, context->queue, mrb_thread_migrate_value(mrb, mrb, arg, context->mrb));
  mrb_queue_unlock(mrb, self);

  check_pthread_error(mrb, pthread_cond_signal(&context->cond));
  return mrb_nil_value();
}

static mrb_value
mrb_queue_shift(mrb_state* mrb, mrb_value self) {
  mrb_value ret;
  mrb_queue_context* context = DATA_PTR(self);
  int len;

  mrb_queue_lock(mrb, self);
  len = RARRAY_LEN(context->queue);
  mrb_queue_unlock(mrb, self);

  if (len == 0) {
    check_pthread_error(mrb, pthread_cond_wait(&context->cond, &context->queue_lock));
  }

  mrb_queue_lock(mrb, self);
  ret = mrb_thread_migrate_value(mrb, context->mrb, mrb_ary_shift(context->mrb, context->queue), mrb);
  mrb_queue_unlock(mrb, self);

  return ret;
}

static mrb_value
mrb_queue_empty_p(mrb_state* mrb, mrb_value self) {
  mrb_bool ret;
  mrb_queue_context* context = DATA_PTR(self);

  mrb_queue_lock(mrb, self);
  ret = RARRAY_LEN(context->queue) == 0;
  mrb_queue_unlock(mrb, self);

  return mrb_bool_value(ret);
}

static mrb_value
mrb_queue_size(mrb_state* mrb, mrb_value self) {
  mrb_int ret;
  mrb_queue_context* context = DATA_PTR(self);

  mrb_queue_lock(mrb, self);
  ret = RARRAY_LEN(context->queue);
  mrb_queue_unlock(mrb, self);

  return mrb_fixnum_value(ret);
}

void
mrb_mruby_thread_gem_init(mrb_state* mrb) {
  struct RClass *_class_thread, *_class_mutex, *_class_queue;

  mrb_define_class(mrb, "ThreadError", mrb->eStandardError_class);

  _class_thread = mrb_define_class(mrb, "Thread", mrb->object_class);
  MRB_SET_INSTANCE_TT(_class_thread, MRB_TT_DATA);
  mrb_define_const(mrb, _class_thread, "COPY_VALUES", mrb_true_value());
  mrb_define_method(mrb, _class_thread, "initialize", mrb_thread_init, MRB_ARGS_OPT(1));
  mrb_define_method(mrb, _class_thread, "join", mrb_thread_join, MRB_ARGS_NONE());
  mrb_define_method(mrb, _class_thread, "kill", mrb_thread_kill, MRB_ARGS_NONE());
  mrb_define_method(mrb, _class_thread, "terminate", mrb_thread_kill, MRB_ARGS_NONE());
  mrb_define_method(mrb, _class_thread, "alive?", mrb_thread_alive, MRB_ARGS_NONE());
  mrb_define_module_function(mrb, _class_thread, "sleep", mrb_thread_sleep, MRB_ARGS_REQ(1));
  mrb_define_module_function(mrb, _class_thread, "usleep", mrb_thread_usleep, MRB_ARGS_REQ(1));
  mrb_define_module_function(mrb, _class_thread, "start", mrb_thread_init, MRB_ARGS_REQ(1));

  _class_mutex = mrb_define_class(mrb, "Mutex", mrb->object_class);
  MRB_SET_INSTANCE_TT(_class_mutex, MRB_TT_DATA);
  mrb_define_method(mrb, _class_mutex, "initialize", mrb_mutex_init, MRB_ARGS_NONE());
  mrb_define_method(mrb, _class_mutex, "lock", mrb_mutex_lock, MRB_ARGS_NONE());
  mrb_define_method(mrb, _class_mutex, "try_lock", mrb_mutex_try_lock, MRB_ARGS_NONE());
  mrb_define_method(mrb, _class_mutex, "locked?", mrb_mutex_locked, MRB_ARGS_NONE());
  mrb_define_method(mrb, _class_mutex, "sleep", mrb_mutex_sleep, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, _class_mutex, "synchronize", mrb_mutex_synchronize, MRB_ARGS_REQ(1));
  mrb_define_method(mrb, _class_mutex, "unlock", mrb_mutex_unlock, MRB_ARGS_NONE());

  _class_queue = mrb_define_class(mrb, "Queue", mrb->object_class);
  MRB_SET_INSTANCE_TT(_class_queue, MRB_TT_DATA);
  mrb_define_method(mrb, _class_queue, "initialize", mrb_queue_init, MRB_ARGS_NONE());
  mrb_define_method(mrb, _class_queue, "clear", mrb_queue_clear, MRB_ARGS_NONE());
  mrb_define_method(mrb, _class_queue, "push", mrb_queue_push, MRB_ARGS_REQ(1));
  mrb_define_alias(mrb, _class_queue, "<<", "push");
  mrb_define_method(mrb, _class_queue, "unshift", mrb_queue_unshift, MRB_ARGS_REQ(1));
  mrb_define_alias(mrb, _class_queue, "enq", "unshift");
  mrb_define_method(mrb, _class_queue, "pop", mrb_queue_pop, MRB_ARGS_OPT(1));
  mrb_define_alias(mrb, _class_queue, "deq", "pop");
  mrb_define_method(mrb, _class_queue, "shift", mrb_queue_shift, MRB_ARGS_OPT(1));
  mrb_define_method(mrb, _class_queue, "size", mrb_queue_size, MRB_ARGS_NONE());
  mrb_define_method(mrb, _class_queue, "empty?", mrb_queue_empty_p, MRB_ARGS_NONE());
}

void
mrb_mruby_thread_gem_final(mrb_state* mrb) {
}

/* vim:set et ts=2 sts=2 sw=2 tw=0: */
